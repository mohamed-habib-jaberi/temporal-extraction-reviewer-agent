import asyncio
import textwrap
from dataclasses import dataclass
from datetime import timedelta
from typing import Optional
import json_repair
import json

from temporalio import workflow
from temporalio.common import RetryPolicy
from temporalio.exceptions import ApplicationError
from temporalio.workflow import ParentClosePolicy

from prompts import _SYNTHESIS_PROMPT, _REVISION_PROMPT

with workflow.unsafe.imports_passed_through():
    from activities import (
        call_llm, CallLLMInput,
    )
    from child_workflow import (
        PDFSummaryWorkflow, PDFSummaryInput
    )

# ── Input / Output ────────────────────────────────────────────────────────────

@dataclass
class ContractReviewInput:
    s3_paths: list
    max_revisions: int = 2

@dataclass
class ContractReviewOutput:
    report: str
    sources: list
    approved_by: str

DEFAULT_RETRY_POLICY = RetryPolicy(
    initial_interval=timedelta(seconds=3),
    backoff_coefficient=2.0,
    maximum_interval=timedelta(seconds=60),
    maximum_attempts=4,
)

@workflow.defn
class ContractReviewWorkflow:

    def __init__(self):
        self._status: str = "processing"
        self._summaries: list = []
        self._report: str = ""

        self._review_decision: Optional[str] = None
        self._review_feedback: str = ""
        self._approved_by: str = ""


    # ── Query: status ───────────────────────────────
    @workflow.query
    def get_status(self) -> dict:

        report = json.dumps(self._report, ensure_ascii=False) if isinstance(self._report, dict) else self._report
        return {
            "status":         self._status,
            "pdfs_processed": len(self._summaries),
            "report_preview": json.dumps(self._report, ensure_ascii=False)[:500],
            "approved_by":    self._approved_by,
        }
    
    # ── Query: full report — call this before submitting a review decision ─────
    @workflow.query
    def get_report(self) -> dict:
        return {
            "status":      self._status,
            "report":      self._report,
            "approved_by": self._approved_by,
            "sources":     [s["s3_path"] for s in self._summaries],
        }

    # ── Signal: record who is reviewing ──────────────────────────────────────

    @workflow.signal
    async def assign_reviewer(self, name: str) -> None:
        self._approved_by = name

    @workflow.update
    async def submit_decision(self, decision: str, feedback: str = "") -> str:
        self._review_decision = decision
        self._review_feedback = feedback

        return f"Decision '{decision}' recorded."
    
    @submit_decision.validator
    def validate_decision(self, decision: str, feedback: str = "") -> None:
        if decision not in ("approve", "revise"):
            raise ValueError(f"Must be 'approve' or 'revise', got: '{decision}'")
        
        if decision == "revise" and not feedback.strip():
            raise ValueError("Feedback is required when requesting a revision.")



    # Coordonne le traitement des PDF, la synthèse, la revue humaine et le résultat final.
    @workflow.run
    async def run(self, params: ContractReviewInput) -> ContractReviewOutput:
        await self._process_pdfs(params.s3_paths)
        await self._synthesize_report()
        await self._review_report(params.max_revisions)
        return self._build_output()

    # Lance un workflow enfant par PDF et conserve les résumés obtenus avec succès.
    async def _process_pdfs(self, s3_paths: list) -> None:
        self._status = "extracting"

        workflow.logger.info(f"Fanning out to {len(s3_paths)} child workflows")

        workflow_id = workflow.info().workflow_id
        workflow_task_queue = workflow.info().task_queue

        # TERMINATE: kill child workflows when parent closes
        # REQUEST_CANCEL: ask child workflows to cancel gracefully
        # ABANDON: leave them alone and let them keep running

        handles = await asyncio.gather(
            *[

               workflow.start_child_workflow(
                   PDFSummaryWorkflow.run ,
                   PDFSummaryInput(
                       s3_path=current_s3_path
                   ),
                   id=f"{workflow_id}-pdf-{idx+1}",
                   task_queue=workflow_task_queue,
                   parent_close_policy=ParentClosePolicy.ABANDON
               )

               for idx, current_s3_path in enumerate(s3_paths)
             ]
        )

        raw_results = await asyncio.gather(
            *handles,
            return_exceptions=True,
        )

        for i, res in enumerate(raw_results):

            if isinstance(res, Exception):
                workflow.logger.warning(f"PDF {i} failed: {res}")
            else:
                self._summaries.append({
                    "s3_path":   res.s3_path,
                    "summary":   res.summary,
                    "key_risks": res.key_risks,
                })

        if len(self._summaries) == 0:
            raise ApplicationError("All PDFs failed to process.")

    # Assemble les résumés et demande au LLM un rapport consolidé des risques.
    async def _synthesize_report(self) -> None:
        self._status = "analyzing"
        workflow.logger.info(f"Synthesizing {len(self._summaries)} summaries")

        combined_summary = "\n\n".join([

            f"**Contract {i+1}** (`{summary['s3_path']}`):\n"
            f"Summary: {summary['summary']}\n"
            f"Risks: {summary['key_risks']}"

            for i, summary in enumerate(self._summaries)
        ])

        llm_prompt = _SYNTHESIS_PROMPT.format(
            summaries=combined_summary,
            n=len(self._summaries)
        )

        self._report = await self._generate_report(llm_prompt)

    # Exécute l’activité LLM avec les délais et retries communs, puis décode le rapport JSON.
    async def _generate_report(self, prompt: str):
        llm_result = await workflow.execute_activity(
            call_llm,
            CallLLMInput(prompt=prompt),
            start_to_close_timeout=timedelta(minutes=3),
            heartbeat_timeout=timedelta(seconds=180),
            retry_policy=DEFAULT_RETRY_POLICY,
        )
        return json_repair.loads(llm_result.content)

    # Gère les cycles de revue jusqu’à une approbation, un timeout ou la fin des cycles.
    async def _review_report(self, max_revisions: int) -> None:
        for revision_no in range(max_revisions + 1):

            self._status = "awaiting-review"
            workflow.logger.info(f"Waiting for human review (cycle {revision_no})")

            if not await self._wait_for_review_decision():
                break

            if self._review_decision == "approve":
                workflow.logger.info(f"Approved by: {self._approved_by}")
                break

            await self._revise_report()

    # Attend une décision humaine pendant trois jours et indique si elle est arrivée à temps.
    async def _wait_for_review_decision(self) -> bool:
        self._review_decision = None
        try:
            await workflow.wait_condition(
                lambda: self._review_decision is not None,
                timeout=timedelta(days=3),
            )
        except asyncio.TimeoutError:
            workflow.logger.warning("Review timed out after 3 days — auto-completing")
            return False
        return True

    # Demande au LLM de réviser le rapport en tenant compte des remarques du réviseur.
    async def _revise_report(self) -> None:
        self._status = "revising"
        workflow.logger.info(f"Revising — feedback: {self._review_feedback}")

        llm_prompt = _REVISION_PROMPT.format(
            report=json.dumps(
                self._report, ensure_ascii=False, indent=2
            ),

            feedback=self._review_feedback,
        )

        self._report = await self._generate_report(llm_prompt)

    # Marque le workflow comme terminé et construit le résultat avec les sources et le réviseur.
    def _build_output(self) -> ContractReviewOutput:
        self._status = "completed"
        return ContractReviewOutput(
            report=self._report,
            sources=[s["s3_path"] for s in self._summaries],
            approved_by=self._approved_by,
        )
