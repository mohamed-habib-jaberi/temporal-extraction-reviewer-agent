"""
Phase 1 — Plain Python PDF-to-Markdown pipeline.
Usage: 
      $ python process_pdf.py s3://temporal-dev/files/Scikit-Learn_Cheat_Sheet.pdf
"""

# Modules inclus dans Python : aucune installation avec pip n'est nécessaire.
import os  # Lit les variables d'environnement, crée des dossiers et supprime des fichiers.
import sys  # Récupère les arguments du terminal, ici le chemin S3 du PDF via sys.argv[1].
import tempfile  # Sert à créer des fichiers ou dossiers temporaires ; non utilisé dans ce script.
import logging  # Affiche les étapes du traitement et les messages de diagnostic.
from pathlib import Path  # Manipule les chemins de fichiers et récupère le nom du PDF.

# Bibliothèques externes : installées avec pip à partir de requirements.txt.
import boto3  # Communique avec S3 (ou IDrive e2) pour télécharger le PDF et envoyer le Markdown.
import pymupdf4llm  # Extrait le contenu du PDF et le convertit au format Markdown.
from dotenv import load_dotenv  # Charge les paramètres du fichier .env dans les variables d'environnement.

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

load_dotenv()

AWS_ACCESS_KEY_ID = os.environ["AWS_ACCESS_KEY_ID"]
AWS_SECRET_ACCESS_KEY = os.environ["AWS_SECRET_ACCESS_KEY"]
AWS_REGION = os.environ["AWS_REGION"]
AWS_S3_ENDPOINT_URL = os.environ["AWS_S3_ENDPOINT_URL"]
S3_BUCKET = os.environ["S3_BUCKET"]
TEMP_DIR = os.environ["TEMP_DIR"]

os.makedirs(TEMP_DIR, exist_ok=True)

# ── S3 helper ────────────────────────────────────────────────────────────────

# Crée un client S3 configuré avec les clés d'accès et l'adresse du service définies dans le fichier .env.
def get_s3_client():
    return boto3.client(
        "s3",
        region_name=AWS_REGION,
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
        endpoint_url=AWS_S3_ENDPOINT_URL,
    )

# Sépare un chemin s3://bucket/fichier.pdf en nom du bucket et clé du fichier dans ce bucket.
def parse_s3_path(s3_path: str):
    s3_path_no_scheme = s3_path.replace("s3://", "")
    bucket, _, key =  s3_path_no_scheme.partition("/")
    return bucket, key

# ── Step 1: Download ─────────────────────────────────────────────────────────
# Télécharge le PDF depuis S3 dans TEMP_DIR et retourne le chemin du fichier local.
def download_pdf(s3_path: str) -> str:
    """Download a PDF from S3. Returns local file path."""
    bucket, key = parse_s3_path(s3_path)
    filename = Path(key).name

    local_path = str(Path(TEMP_DIR) / filename)

    log.info(f"Downloading: s3://{bucket}/{key} => {local_path}")

    s3_client = get_s3_client()
    s3_client.download_file(
        bucket,
        key,
        local_path
    )

    log.info(f"COMPLETED Downloading : {local_path} ")
    return local_path

# ── Step 2: Extract to Markdown ───────────────────────────────────────────────
# Extrait le contenu du PDF local et retourne le texte converti au format Markdown.
def extract_to_markdown(local_pdf_path: str) -> str:
    """Extract text from PDF and convert to Markdown. Returns markdown string."""
    log.info(f"Extracting text from {local_pdf_path}")
    markdown_text = pymupdf4llm.to_markdown(local_pdf_path)

    log.info(f"Extraction complete — {len(markdown_text)} characters")
    return markdown_text

# ── Step 3: Upload Markdown ──────────────────────────────────────────────────
# Enregistre le Markdown dans le bucket du PDF avec une clé .md et retourne son chemin S3.
def upload_markdown(markdown_text: str, original_s3_path: str) -> str:
    """Upload markdown content to S3. Returns the output S3 path."""
    bucket, key = parse_s3_path(original_s3_path)
    md_key = key.replace(".pdf", ".md")

    log.info(f"Uploading markdown → s3://{bucket}/{md_key}")
    s3_client = get_s3_client()

    s3_client.put_object(
        Bucket=bucket,
        Key=md_key,

        Body=markdown_text.encode("utf-8"),
        ContentType="text/markdown",
    )

    output_path = f"s3://{bucket}/{md_key}"
    log.info(f"Upload complete: {output_path}")
    return output_path

# ── Main pipeline ─────────────────────────────────────────────────────────────
# Enchaîne le téléchargement, l'extraction et l'envoi du Markdown, supprime le PDF local et retourne le chemin S3 du résultat.
def process_pdf(s3_input_path: str) -> str:
    """Run the full pipeline. Returns the output S3 path."""

    log.info(f"Starting pipeline for: {s3_input_path}")

    local_pdf  = download_pdf(s3_input_path)
    markdown   = extract_to_markdown(local_pdf)
    output_s3  = upload_markdown(markdown, s3_input_path)

    # Clean up temp file
    os.remove(local_pdf)

    log.info(f"Pipeline complete. Output: {output_s3}")
    return output_s3

if __name__ == "__main__":


    output_s3 = process_pdf(
        sys.argv[1]
    )

    print(f"\nDone! Markdown saved to: {output_s3}")

