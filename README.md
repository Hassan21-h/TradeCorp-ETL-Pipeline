# 🚀 TradeCorp International - Pipeline Data ETL Industrialisé

![Docker](https://img.shields.io/badge/Docker-Compose-blue?logo=docker)
![PySpark](https://img.shields.io/badge/PySpark-3.5-orange?logo=apachespark)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-16-blue?logo=postgresql)
![Apache Airflow](https://img.shields.io/badge/Apache%20Airflow-2.8-teal?logo=apacheairflow)
![Azure ADLS Gen2](https://img.shields.io/badge/Azure-ADLS%20Gen2-0078D4?logo=microsoftazure)
![Pytest](https://img.shields.io/badge/Pytest-9.1-green?logo=pytest)

---

## 📌 Contexte & Enjeux Métier
Chaque nuit, **TradeCorp International** reçoit ses données commerciales sous forme de fichiers CSV bruts. Le traitement était jusqu'à présent réalisé manuellement chaque matin (environ 3 heures de traitement), générant des erreurs, un manque d'historisation et une absence totale de traçabilité.

**Objectif du projet :** Industrialiser un pipeline ETL PySpark conteneurisé qui télécharge les fichiers métiers et référentiels depuis Azure ADLS Gen2, applique des transformations complexes (nettoyage, calculs financiers, conversion dynamique de devises), persiste les données enrichies en format Parquet et garantit la qualité via une suite de tests unitaires automatisés. L'ensemble est orchestré quotidiennement par **Apache Airflow**.

---

## 🏗️ Architecture Technique Target

``` mermaid
flowchart TD
                                    ┌──────────────────────────────────┐
                                    │   Azure ADLS Gen2 (Zone raw)     │
                                    │  - Données métiers CSV           │
                                    │  - country_currency.csv          │
                                    │  - exchange_rates.json           │
                                    └────────────────┬─────────────────┘
                                                     │
                                                     ▼
┌────────────────────────────────┐       ┌──────────────────────────────────┐
│   External Rates API           │ ───>  │   Ingestion & Cache Local        │
│  (Taux de change en temps réel)│       │   (data/ & data/reference/)      │
└────────────────────────────────┘       └────────────────┬─────────────────┘
                                                          │
                                                          ▼
┌───────────────────────────────────────────────────────────────────────────┐
│   PySpark Execution Engine (src/)                                         │
│   ├── reader.py      : Ingestion & application du MapType schema          │
│   ├── transformer.py : Clean orders, customers & calcul sous-totaux       │
│   ├── enrichment.py  : Join country & crossJoin rates (element_at)        │
│   └── writer.py       : Écriture Parquet & Upload Cloud                   │
└────────────────────────────────┬──────────────────────────────────────────┘
                                 │
                 ┌───────────────┴───────────────┐
                 ▼                               ▼
┌────────────────────────────────┐   ┌──────────────────────────────────┐
│  Automated Testing (tests/)    │   │   Azure ADLS Gen2 (Zone clean)   │
│  - Mocks PySpark (sans réseau) │   │   - Dataset enrichi en Parquet   │
│  - Execution via PyTest        │   │   - Partitionné par pays         │
└────────────────────────────────┘   └──────────────────────────────────┘

```

---

## 🛠️ Stack Technique & Justifications

* **Docker & Dev Containers** : Garantit la reproductibilité exacte de l'environnement de développement et de production.
* **Apache Spark (PySpark)** : Moteur de calcul distribué pour le traitement à grande échelle, l'enrichissement et les agrégations complexes (**Window Functions**).
* **Format Parquet & Partitionnement** : Format de stockage en colonnes hautement compressé.
* **PostgreSQL (Driver JDBC `42.7.0`)** : Entrepôt de données relationnel pour l'exposition des KPI métiers.
* **Azure ADLS Gen2 & Key Vault** : Stockage Cloud Data Lake centralisé (zones `raw` / `clean`) et gestion ultra-sécurisée des secrets.
* **Apache Airflow** : Orchestration automatisée, gestion des dépendances et suivi des exécutions.
* **Pytest** : Validation de la qualité du code via des tests unitaires automatisés.

---

## 📂 Structure du Dépôt

```
tradecorp/
├── .env                       # Variables d'environnement & identifiants Azure (ignoré)
├── .gitignore                 # Exclusion des logs, environnements et caches
├── README.md                  # Documentation du projet
├── Dockerfile                 # Image conteneurisée PySpark
├── Dockerfile.airflow         # Image Airflow (webserver + scheduler)
├── docker-compose.yml         # Orchestration des services (spark, postgres, airflow)
├── requirements.txt           # Dépendances Python (pyspark, azure-storage-blob, pytest)
├── data/                      # Stockage local des fichiers (ignoré par Git)
│   ├── clean/                 # Sorties nettoyées et formatées
│   ├── output/                # Fichiers de restitution finalisés
│   ├── raw/                   # Zone de réception des données brutes
│   ├── reference/             # Référentiels (country_currency.csv, exchange_rates.json)
│   ├── staging/                # Tables métier nettoyées, un Parquet par table
│   ├── transformed/            # Résultat enrichi produit par transformer.py
│   ├── tmp/                   # Fichiers temporaires de traitement
│   ├── categories.csv         # Référentiel catégories produits
│   ├── customers.csv          # Données clients
│   ├── employees.csv          # Données employés
│   ├── order_details.csv      # Lignes de détail des commandes
│   ├── orders.csv             # Entêtes de commandes
│   ├── products.csv           # Catalogue produits
│   ├── shippers.csv           # Transporteurs
│   └── suppliers.csv          # Fournisseurs
├── dags/                      # DAGs Airflow
│   └── tradecorp_pipeline.py  # Orchestration du pipeline ETL complet
├── notebooks/                 # Exploration et prototypage
├── src/                       # Code source modulaire du pipeline
│   ├── reader.py              # Ingestion Azure ADLS & création SparkSession
│   ├── transformer.py         # Métrique métier et calculs sous-totaux
│   ├── fetch_exchange_rates.py # Récupération des taux de change temps réel
│   ├── writer.py               # Export Parquet & Upload Azure ADLS
│   └── utils.py                # Fonctions de nettoyage réutilisables (clean_*)
└── tests/                     # Suite de tests unitaires automatisés
    ├── run_tests.py            # Runner de tests
    └── test_transformers.py    # Tests unitaires PySpark (mock DataFrame & devises)

```

---

## ⚡ Prise en Main Rapide

### 1. Prérequis
* Docker Desktop installé et démarré.
* Git configuré sur votre machine.
* Fichier `.env` renseigné à la racine avec les identifiants Azure (`account_name`, `account_key`).

### 2. Démarrage de l'infrastructure

Lancer l'ensemble des conteneurs (spark, postgres, airflow) :

```bash
docker compose up --build
```

Vérifier le statut :

```bash
docker compose ps
```

Suivre les logs d'un service en particulier, par exemple Airflow au démarrage :

```bash
docker logs -f tradecorp_airflow
```

Pour repartir d'un environnement totalement propre (supprime aussi la base Postgres, y compris les métadonnées Airflow) :

```bash
docker compose down -v
docker compose up --build
```

Un simple `docker compose down` (sans `-v`) arrête les conteneurs mais conserve les données du volume `pgdata` — à privilégier pour un redémarrage courant.

### 3. Accès aux interfaces web
* **JupyterLab :** `http://localhost:8888`
* **Spark UI :** `http://localhost:4041`
* **PostgreSQL (DBeaver) :** `localhost:5454` *(User: tradecorp, Database: tradecorp)*
* **Apache Airflow :** `http://localhost:8081` *(User: admin, Password: admin)*

---

## 🔄 Orchestration Airflow

Le pipeline ETL est déclenché automatiquement chaque jour à **6h00 UTC** (`schedule_interval="0 6 * * *"`) par le DAG `tradecorp_etl_pipeline`, défini dans `dags/tradecorp_pipeline.py`.

### Enchaînement des tâches

```
fetch_exchange_rates  →  reader  →  transformer  →  writer
     (BashOperator)      (DockerOperator)  (DockerOperator)  (DockerOperator)
```

| Tâche | Opérateur | Rôle |
|---|---|---|
| `fetch_exchange_rates` | `BashOperator` | Récupère les derniers taux de change via l'API `exchangerate-api.com` et les dépose dans `raw/reference/exchange_rates.json` sur Azure. |
| `reader` | `DockerOperator` | Télécharge les CSV métiers et les référentiels depuis la zone `raw` d'Azure ADLS, puis les écrit en Parquet dans `data/staging/`. |
| `transformer` | `DockerOperator` | Nettoie et enrichit les données (jointures clients/commandes/produits, calcul des sous-totaux) puis écrit le résultat dans `data/transformed/`. |
| `writer` | `DockerOperator` | Upload le dataset enrichi vers la zone `clean` d'Azure ADLS (container `clean`, prefix `orders_enriched`). |

Les tâches `DockerOperator` s'exécutent dans des conteneurs Spark éphémères (image `tradecorp-spark`), lancés côte à côte du conteneur Airflow via le socket Docker de l'hôte (`/var/run/docker.sock`). Les identifiants Azure (`account_name`, `account_key`) sont transmis explicitement à chaque conteneur enfant via le paramètre `environment` de `DockerOperator`, et les répertoires `src/`, `data/` sont partagés par bind mount pour l'échange de fichiers entre les étapes.

### Déclencher ou relancer le DAG

**Depuis l'interface web** (`http://localhost:8081`) : sur la page listant les DAGs, basculer le toggle à gauche de `tradecorp_etl_pipeline` sur **ON** — tout nouveau DAG est mis en pause à sa création, il ne s'exécute pas tant qu'il n'a pas été activé manuellement. Cliquer ensuite sur le bouton **▶ (Trigger DAG)** pour lancer un run immédiat, sans attendre l'échéance cron.

**En ligne de commande**, depuis le conteneur Airflow :

```bash
docker exec -it tradecorp_airflow airflow dags trigger tradecorp_etl_pipeline
```

**Relancer une tâche précise après un échec** (par exemple si `reader` est passé en `failed`), plutôt que de relancer tout le DAG depuis le début : dans l'UI, cliquer sur la tâche concernée puis sur **Clear** — Airflow ne réexécute que cette tâche et celles qui en dépendent en aval (`transformer`, `writer`), pas les tâches déjà réussies en amont.

**Suivre l'exécution en direct** :

```bash
docker logs -f tradecorp_airflow
```

Ou, pour les logs d'une tâche précise, cliquer sur son rectangle dans le graphe du DAG (UI) puis sur **Logs**.

### Le rôle de `catchup`

Le DAG est configuré avec `catchup=False`. Par défaut, Airflow considère qu'un DAG doit rattraper **toutes les exécutions passées** entre sa `start_date` et aujourd'hui dès qu'il est activé — par exemple, si `start_date=datetime(2024, 1, 1)` et que le DAG est activé le 8 septembre 2026, Airflow tenterait de déclencher un run pour **chaque jour manqué** depuis 2024, soit plus de 900 exécutions d'un coup.

`catchup=False` désactive ce comportement : à l'activation, Airflow ne planifie que le **prochain** run à venir selon le `schedule_interval` (ici, le prochain 6h00 UTC), sans chercher à combler l'historique. C'est le réglage attendu ici, puisque le pipeline traite des données du jour (taux de change, commandes) — rejouer des runs pour des dates passées n'aurait pas de sens métier et surchargerait inutilement Postgres, Azure et les workers Spark au premier démarrage.

---

## 📋 Suivi de Projet & Méthodologie Agile

Le projet est piloté selon la méthode Kanban répartie sur 3 jalons principaux

### État d'avancement des Jalons :
- [x] **Jalon 1 :** Cadrage, étude des risques, conteneurisation Docker, exploration des données et ingestion.
- [x] **Jalon 2 :** Transformations PySpark, Window Functions, export JDBC Postgres et écriture Parquet.
- [x] **Jalon 3 :** Orchestration Airflow, modularisation du code et documentation finale.

### Extraits de logs — preuves d'exécution du DAG

**`fetch_exchange_rates`** — nombre de devises récupérées depuis l'API :

```
[2026-09-08, 11:57:27 UTC] {subprocess.py:93} INFO - 2026-09-08 11:57:27,546 [INFO] 166 devises récupérées depuis l'API.
```

**`writer`** — confirmation d'upload vers ADLS Gen2 (zone `clean`) :

```
[2026-09-08, 11:58:32 UTC] {docker.py:429} INFO - 2026-09-08 11:58:32,897 [INFO] Upload terminé vers ADLS : container='clean', prefix='orders_enriched'
```
