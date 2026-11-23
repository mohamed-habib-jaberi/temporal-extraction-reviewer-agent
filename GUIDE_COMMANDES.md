# Guide des commandes — installer, lancer et vérifier le projet

Ce guide accompagne le cours dans l'ordre : extraction Python simple, extraction avec Temporal, puis analyse IA des contrats. Les commandes ci-dessous ciblent un terminal macOS ou Linux (Bash/Zsh) et une installation locale. Sur Windows, utilisez WSL ou adaptez les commandes à PowerShell.

## Sommaire

1. [Prérequis et dossier du projet](#1-prérequis-et-dossier-du-projet)
2. [Cloner et démarrer le serveur Temporal](#2-cloner-et-démarrer-le-serveur-temporal)
3. [Installer la CLI Temporal](#3-installer-la-cli-temporal)
4. [Créer un environnement Python](#4-créer-un-environnement-python)
5. [Configurer S3 et les fichiers .env](#5-configurer-s3-et-les-fichiers-env)
6. [Exemple 1 : Python sans Temporal](#6-exemple-1--python-sans-temporal)
7. [Exemple 2 : PDF avec Temporal et FastAPI](#7-exemple-2--pdf-avec-temporal-et-fastapi)
8. [Exemple 3 : revue de contrats avec IA](#8-exemple-3--revue-de-contrats-avec-ia)
9. [Arrêter et relancer les composants](#9-arrêter-et-relancer-les-composants)
10. [Dépannage et vérifications](#10-dépannage-et-vérifications)

## 1. Prérequis et dossier du projet

Installez Git, [Docker Desktop](https://www.docker.com/products/docker-desktop/) sur Mac (Docker Engine et Compose sur Linux), et [Miniconda](https://www.anaconda.com/docs/getting-started/miniconda/install) si vous choisissez Conda. Lancez Docker avant de continuer.

```bash
git --version
docker --version
docker compose version
docker info
```

Ouvrez le dossier du cours téléchargé ou cloné. Depuis sa racine, définissez cette variable **dans chaque nouveau terminal** :

```bash
export COURSE_ROOT="$PWD"
```

Si le terminal est déjà dans un sous-dossier, revenez d'abord à la racine. Sur la machine utilisée pour ce cours, le chemin est :

```bash
cd /Users/ET35771/Documents/AI/project/temporal-extraction-reviewer-agent
export COURSE_ROOT="$PWD"
```

Les autres étudiants doivent remplacer ce chemin par celui de leur copie. Toutes les commandes suivantes utilisent `COURSE_ROOT` pour éviter les ambiguïtés de dossier.

## 2. Cloner et démarrer le serveur Temporal

Le serveur est fourni par un dépôt séparé. Le dossier attendu s'appelle **`setup/samples-server`**, avec un « s » à `samples`.

Depuis la racine, uniquement si ce dépôt n'a pas encore été récupéré :

```bash
cd "$COURSE_ROOT/setup"
git clone https://github.com/temporalio/samples-server.git
```

Un dossier `samples-server` déjà présent doit être vide pour cette commande. S'il contient déjà le dépôt, passez directement à la suite ; ne le supprimez pas.

Le dépôt récupéré contient son propre `.env` dans `compose`, avec les versions des images Docker. Ce fichier est distinct des `.env` des applications Python. S'il manque, consultez le README du dépôt récupéré pour la configuration de sa version.

Vérifiez puis lancez la configuration PostgreSQL :

```bash
cd "$COURSE_ROOT/setup/samples-server/compose"
docker compose -f docker-compose-postgres.yml config --quiet
docker compose -f docker-compose-postgres.yml up -d
docker compose -f docker-compose-postgres.yml ps -a
```

Le premier démarrage télécharge les images et initialise la base. Temporal et PostgreSQL doivent devenir sains ; les conteneurs d'initialisation peuvent terminer avec le code `0`, ce qui est normal.

| Service | Adresse locale | Utilité |
|---|---|---|
| Temporal | `localhost:7233` | Connexion des workers, de l'API et de la CLI. |
| Interface web | [http://localhost:8080](http://localhost:8080) | Consultation des workflows. |
| PostgreSQL | `localhost:5432` | Stockage interne du serveur Temporal. |

L'interface peut afficher **0 Workflows** avant le premier traitement. Cela ne signifie pas que le serveur est en panne.

Pour consulter les logs :

```bash
docker compose -f docker-compose-postgres.yml logs --tail=100 temporal
```

Sur Mac, les étapes de la vidéo avec `/etc/systemd/system`, `systemctl` et les fichiers `.service` ne s'appliquent pas. Un tunnel SSH n'est pas nécessaire quand Temporal tourne sur le même ordinateur.

## 3. Installer la CLI Temporal

La CLI est un outil de terminal distinct du serveur Docker et de la bibliothèque Python `temporalio`.

Sur Mac avec Homebrew, si elle n'est pas déjà installée :

```bash
brew install temporal
```

Pour les autres systèmes ou une installation sans Homebrew, suivez les [instructions officielles de la CLI](https://docs.temporal.io/cli#install).

Vérifiez l'installation et la connexion au serveur :

```bash
temporal --version
temporal operator cluster health --address localhost:7233
temporal operator namespace list --address localhost:7233
temporal workflow list --address localhost:7233 --namespace default
```

Le namespace `default` est créé par la configuration Docker utilisée ici. Les commandes de ce guide utilisent explicitement ce namespace et l'adresse locale.

## 4. Créer un environnement Python

Choisissez **une** des deux options. Un seul environnement partagé suffit pour les exemples de ce cours.

### Option A : Conda, comme dans la vidéo

```bash
conda --version
conda create -n temporaldev python=3.11
conda activate temporaldev
python --version
```

Si `temporaldev` existe déjà, activez-le sans le recréer. Si vous avez créé `py311` et voulez le renommer :

```bash
conda deactivate
conda rename -n py311 temporaldev
conda activate temporaldev
```

### Option B : venv, sans Conda

Avec Python 3.11 ou plus récent installé :

```bash
cd "$COURSE_ROOT"
python3 --version
python3 -m venv .venv
source .venv/bin/activate
```

### Installer les dépendances

Dans l'environnement choisi :

```bash
python -m pip install -r "$COURSE_ROOT/apps/pdf-extraction-01-plain/requirements.txt"
python -m pip install -r "$COURSE_ROOT/apps/pdf-extraction-02-temporal/requirements.txt"
python -m pip install -r "$COURSE_ROOT/apps/client-app/requirements.txt"
python -m pip install -r "$COURSE_ROOT/apps/ai-contract-review/requirements.txt"
python -m pip check
```

Vous pouvez installer chaque fichier au moment d'aborder son exemple. Réactivez l'environnement dans chaque terminal qui exécutera du Python.

## 5. Configurer S3 et les fichiers .env

Dans la [console IDrive e2](https://console.idrivee2.com/access-key), créez un bucket et une clé autorisant la lecture et l'écriture sur ce bucket. Relevez la région et l'endpoint S3 exacts. Importez un PDF, par exemple sous la clé `files/Scikit-Learn_Cheat_Sheet.pdf`.

Depuis la racine, créez les `.env` manquants sans écraser ceux déjà configurés :

```bash
cd "$COURSE_ROOT"
for app_dir in pdf-extraction-01-plain pdf-extraction-02-temporal client-app ai-contract-review; do
    if [ ! -f "apps/$app_dir/.env" ]; then
        cp "apps/$app_dir/.env.example" "apps/$app_dir/.env"
    fi
done
```

### Paramètres S3

Dans les `.env` des deux applications PDF et de `ai-contract-review`, remplacez les valeurs d'exemple :

```dotenv
AWS_ACCESS_KEY_ID=votre-access-key
AWS_SECRET_ACCESS_KEY=votre-secret-key
AWS_REGION=region-fournie-par-idrive
AWS_S3_ENDPOINT_URL=https://endpoint-fourni-par-idrive
S3_BUCKET=temporal-dev
TEMP_DIR=/chemin/absolu/vers/le/projet/tmp/pdf-pipeline
```

Le chemin `/chemin/absolu/...` doit être remplacé par un véritable chemin sur votre machine. Pour utiliser un dossier partagé dans le projet :

```bash
mkdir -p "$COURSE_ROOT/tmp/pdf-pipeline"
printf '%s\n' "$COURSE_ROOT/tmp/pdf-pipeline"
```

Copiez le chemin affiché dans `TEMP_DIR` ; n'écrivez pas littéralement `$COURSE_ROOT` dans le `.env`. Un dossier temporaire distinct par application est également possible. Le worker IA attend que son dossier existe déjà.

Le bucket effectivement traité vient de l'argument `s3://bucket/clé`, même si `S3_BUCKET` est également requis dans certains fichiers de configuration. Ne publiez pas les `.env` ni les clés secrètes. Les `.env.example` restent des modèles sans secrets.

### Paramètres Temporal

Dans `apps/pdf-extraction-02-temporal/.env` :

```dotenv
TEMPORAL_HOST=localhost:7233
TEMPORAL_NAMESPACE=default
TEMPORAL_PDF_PROCESS_TASK_QUEUE=pdf-pipeline-queue
```

Dans `apps/client-app/.env`, les quatre variables sont nécessaires, même si vous testez uniquement les PDF :

```dotenv
TEMPORAL_HOST=localhost:7233
TEMPORAL_NAMESPACE=default
TEMPORAL_PDF_PROCESS_TASK_QUEUE=pdf-pipeline-queue
TEMPORAL_CONTRACT_REVIEW_TASK_QUEUE=contract-review-queue
```

Dans `apps/ai-contract-review/.env`, ajoutez les paramètres Temporal et OpenRouter aux paramètres S3 :

```dotenv
TEMPORAL_HOST=localhost:7233
TEMPORAL_NAMESPACE=default
TEMPORAL_TASK_QUEUE=contract-review-queue
OPENROUTER_API_KEY=votre-cle-openrouter
OPENROUTER_MODEL=openai/gpt-4o-mini
```

Obtenez la clé depuis [OpenRouter](https://openrouter.ai/). Le traitement IA transmet des extraits des documents au fournisseur du modèle et peut consommer des crédits.

Les task queues doivent correspondre entre client et worker. Lancez toujours les scripts depuis leur dossier d'application pour charger la configuration attendue. Après modification d'un `.env`, redémarrez le processus concerné ; des variables déjà exportées dans le terminal peuvent prendre priorité sur le fichier.

## 6. Exemple 1 : Python sans Temporal

**Un seul terminal Python.** Aucun serveur Temporal, worker ou API n'est nécessaire.

```bash
conda activate temporaldev
cd "$COURSE_ROOT/apps/pdf-extraction-01-plain"
python process_pdf.py s3://temporal-dev/files/Scikit-Learn_Cheat_Sheet.pdf
```

Avec venv, remplacez l'activation Conda par `source "$COURSE_ROOT/.venv/bin/activate"` dans cet exemple et les suivants.

Remplacez le bucket et la clé par ceux de votre PDF. Le résultat attendu est :

```text
Done! Markdown saved to: s3://temporal-dev/files/Scikit-Learn_Cheat_Sheet.md
```

Vérifiez dans IDrive e2 que le `.md` existe et contient du texte. Il est envoyé dans S3, pas enregistré localement. Le PDF temporaire est supprimé après un traitement réussi. Relancer le même traitement peut remplacer le Markdown à la même clé.

L'explication détaillée se trouve dans [le README du premier exemple](apps/pdf-extraction-01-plain/README.md).

## 7. Exemple 2 : PDF avec Temporal et FastAPI

Temporal doit être démarré. Ouvrez **trois terminaux** : un pour le worker, un pour l'API, un pour les commandes de test. Dans chaque nouveau terminal, définissez `COURSE_ROOT` comme à l'étape 1.

### Terminal A : worker PDF

```bash
conda activate temporaldev
cd "$COURSE_ROOT/apps/pdf-extraction-02-temporal"
python worker.py
```

Laissez ce terminal ouvert. Le message de démarrage doit mentionner `pdf-pipeline-queue`.

### Terminal B : API FastAPI

```bash
conda activate temporaldev
cd "$COURSE_ROOT/apps/client-app"
python -m uvicorn main:app --reload --port 5000
```

Laissez aussi ce terminal ouvert. La documentation interactive est disponible sur [http://localhost:5000/docs](http://localhost:5000/docs).

### Terminal C : vérifier et lancer un PDF

**Uvicorn démarre l'API ; curl lui envoie des demandes.**

- `uvicorn main:app --reload --port 5000` lance l'API et la recharge quand le code change.
- `curl .../health` vérifie que l'API répond, sans traiter de PDF.
- `curl -X POST .../process-pdf/execute` envoie le chemin du PDF en JSON ; l'API lance le workflow Temporal et attend son résultat.

Gardez l'API et le worker actifs dans leurs terminaux, puis lancez les commandes `curl` dans un autre terminal.

```bash
curl -f http://localhost:5000/health
curl -f -X POST http://localhost:5000/process-pdf/execute \
    -H 'Content-Type: application/json' \
    -d '{"s3_path":"s3://temporal-dev/files/Scikit-Learn_Cheat_Sheet.pdf"}'
```

`/health` vérifie que l'API répond, pas la disponibilité de Temporal. La requête `/process-pdf/execute` attend la fin du traitement et retourne un `workflow_id` et le chemin S3 dans `results`. Vérifiez également le workflow dans [Temporal Web UI](http://localhost:8080/namespaces/default/workflows) et le `.md` dans IDrive e2.

Sur Mac, si `localhost:5000` renvoie une réponse vide ou `403` avec le serveur `AirTunes`, il atteint le service AirPlay plutôt que FastAPI. Utilisez `127.0.0.1` à la place de `localhost` dans les URL de l'API, y compris pour les requêtes POST et `/docs` :

```bash
curl -i http://127.0.0.1:5000/health
```

La réponse attendue est `HTTP/1.1 200 OK` avec `{"status":"ok"}`.

### Alternative : démarrer avec la CLI, sans API

Le worker du terminal A doit rester actif. Un nouvel identifiant est généré pour chaque essai :

```bash
export PDF_WORKFLOW_ID="pdf-test-$(uuidgen)"
temporal workflow start --address localhost:7233 --namespace default \
    --workflow-id "$PDF_WORKFLOW_ID" \
    --type PDFPipelineWorkflow \
    --task-queue pdf-pipeline-queue \
    --input '{"s3_path":"s3://temporal-dev/files/Scikit-Learn_Cheat_Sheet.pdf"}'
temporal workflow describe --address localhost:7233 --namespace default \
    --workflow-id "$PDF_WORKFLOW_ID"
temporal workflow result --address localhost:7233 --namespace default \
    --workflow-id "$PDF_WORKFLOW_ID"
```

`describe` consulte l'état ; `result` attend la fin. Dans la version actuelle de `main.py`, les routes `/process-pdf/start` et `/workflow/status/{workflow_id}` sont commentées : utilisez cette alternative CLI pour le démarrage asynchrone et le suivi.

Pour ce cours, utilisez un seul worker PDF sur votre machine : les activités se transmettent un chemin de fichier local, qui ne serait pas nécessairement accessible sur une autre machine.

## 8. Exemple 3 : revue de contrats avec IA

Prérequis : Temporal actif, configuration S3 et OpenRouter renseignée, dossier `TEMP_DIR` existant et contrats importés dans votre bucket. Les exemples de PDF sont dans `apps/ai-contract-review/samples` ; importez-les via la console S3 dans `legal-docs/` pour utiliser les chemins ci-dessous.

### Terminal A : worker IA

```bash
conda activate temporaldev
cd "$COURSE_ROOT/apps/ai-contract-review"
python worker.py
```

Le worker écoute `contract-review-queue`. Les workflows parent et enfants sont exécutés par ce worker. L'API n'est pas nécessaire pour les commandes CLI suivantes.

### Terminal B : démarrer et consulter la revue

```bash
export REVIEW_WORKFLOW_ID="contract-test-$(uuidgen)"
temporal workflow start --address localhost:7233 --namespace default \
    --workflow-id "$REVIEW_WORKFLOW_ID" \
    --type ContractReviewWorkflow \
    --task-queue contract-review-queue \
    --input '{"s3_paths":["s3://temporal-dev/legal-docs/vendor-service-agreement.pdf","s3://temporal-dev/legal-docs/nda-innovate-consultpro.pdf","s3://temporal-dev/legal-docs/software-license-globalsoft.pdf"],"max_revisions":2}'
temporal workflow query --address localhost:7233 --namespace default \
    --workflow-id "$REVIEW_WORKFLOW_ID" --name get_status
temporal workflow query --address localhost:7233 --namespace default \
    --workflow-id "$REVIEW_WORKFLOW_ID" --name get_report
```

Attendez l'état **`awaiting-review`**, puis lisez le rapport avant de transmettre une décision. Réexécutez les queries pour suivre la progression. Les enfants et leurs activités apparaissent aussi dans l'interface Temporal.

### Assigner un réviseur

```bash
temporal workflow signal --address localhost:7233 --namespace default \
    --workflow-id "$REVIEW_WORKFLOW_ID" --name assign_reviewer \
    --input '"Étudiant"'
```

### Demander une révision, si nécessaire

```bash
temporal workflow update execute --address localhost:7233 --namespace default \
    --workflow-id "$REVIEW_WORKFLOW_ID" --name submit_decision \
    --input '"revise"' --input '"Rédige le rapport en français."'
```

Les deux `--input` correspondent aux deux arguments : décision et commentaire. Après une révision, attendez à nouveau `awaiting-review` et consultez le rapport.

### Approuver et récupérer le résultat

```bash
temporal workflow update execute --address localhost:7233 --namespace default \
    --workflow-id "$REVIEW_WORKFLOW_ID" --name submit_decision \
    --input '"approve"' --input '""'
temporal workflow result --address localhost:7233 --namespace default \
    --workflow-id "$REVIEW_WORKFLOW_ID"
```

Le résultat contient le rapport, les sources traitées et le nom enregistré pour le réviseur. Cette version retourne le rapport via Temporal ; elle ne l'envoie pas dans S3.

**État actuel du code :** les routes `/contract-review/...` sont commentées dans `apps/client-app/main.py`. Les commandes CLI ci-dessus permettent d'interagir avec le workflow sans les réactiver. Le nom du réviseur n'est pas une identité authentifiée. Le code peut terminer sans approbation après expiration du délai ou des cycles de revue ; `COMPLETED` ne prouve donc pas à lui seul une validation humaine. L'analyse utilise les 5 000 premiers caractères de chaque PDF et reste une démonstration pédagogique.

## 9. Arrêter et relancer les composants

Dans les terminaux de l'API et des workers, utilisez **Ctrl+C**. Arrêter un worker ne supprime pas les workflows : ils peuvent attendre son retour. Relancez-le avec la même configuration pour reprendre le traitement.

Pour arrêter les conteneurs tout en les conservant :

```bash
cd "$COURSE_ROOT/setup/samples-server/compose"
docker compose -f docker-compose-postgres.yml stop
```

Pour les relancer :

```bash
docker compose -f docker-compose-postgres.yml up -d
```

Le guide utilise `stop` pour conserver les conteneurs et leurs volumes. Ne supprimez pas les volumes si vous souhaitez conserver l'historique Temporal.

Pour quitter l'environnement Python : `conda deactivate` avec Conda, ou `deactivate` avec venv.

## 10. Dépannage et vérifications

| Symptôme | Vérification ou action |
|---|---|
| `docker` ne répond pas | Démarrer Docker Desktop, puis lancer `docker info`. |
| Clonage refusé : dossier non vide | Vérifier si `setup/samples-server` contient déjà le dépôt. |
| Port 8080, 7233 ou 5432 occupé | Identifier le service existant ; vérifier `docker ps` et les ports affichés. |
| `conda: command not found` | Installer/initialiser Conda, ou choisir venv. |
| `ModuleNotFoundError` | Activer l'environnement et installer le `requirements.txt` de l'application. |
| `KeyError` sur une variable | Vérifier le `.env` de l'application et le dossier depuis lequel le script est lancé. |
| `AccessDenied` ou fichier S3 absent | Vérifier endpoint, région, droits, bucket et clé exacte, y compris les majuscules. |
| Workflow en attente | Vérifier qu'un worker écoute la bonne task queue dans le namespace `default`. |
| Erreur OpenRouter | Vérifier la clé, le modèle et les crédits du compte sans afficher la clé dans les logs. |
| Erreur de dossier temporaire dans le worker IA | Créer le dossier exact renseigné dans `TEMP_DIR`. |
| API : réponse 404 sur une route | Consulter `/docs` ; plusieurs routes du cours sont actuellement commentées. |

Depuis la racine, vérification des dépendances et de la syntaxe Python :

```bash
python -m pip check
python -m compileall -q "$COURSE_ROOT/apps"
```

Ces commandes ne testent pas les connexions S3 ou LLM. Le projet ne fournit pas encore de suite de tests automatisés ; les traitements et contrôles ci-dessus sont des tests manuels d'intégration, qui lisent/écrivent dans S3 et, pour la revue IA, appellent le modèle.

### Fichiers à exécuter et fichiers de support

| Fichier | Commande / rôle |
|---|---|
| `pdf-extraction-01-plain/process_pdf.py` | `python process_pdf.py s3://bucket/clé.pdf` : traitement direct. |
| `pdf-extraction-02-temporal/worker.py` | `python worker.py` : worker PDF, à laisser actif. |
| `pdf-extraction-02-temporal/workflow_process_pdf.py` | Workflow chargé par le worker ; ne pas lancer directement. |
| `pdf-extraction-02-temporal/activities.py` et `helpers.py` | Activités et paramètres utilisés par le worker. |
| `client-app/main.py` | `python -m uvicorn main:app --reload --port 5000` : API. |
| `ai-contract-review/worker.py` | `python worker.py` : worker d'analyse IA, à laisser actif. |
| `ai-contract-review/parent_workflow.py` et `child_workflow.py` | Workflows chargés par le worker IA. |
| `ai-contract-review/activities.py` et `prompts.py` | Extraction, appels LLM et modèles de prompts. |
| `requirements.txt` | `python -m pip install -r requirements.txt`, depuis l'application. |
| `.env` | Configuration locale chargée au démarrage. |
| `setup/services/temporal.service` et `temporal-worker.service` | Exemples systemd Linux avec chemins à adapter ; inutiles sur Mac. |
