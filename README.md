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

```
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
* Lancer l'ensemble des conteneurs : `docker compose up --build`
* Vérifier le statut : `docker compose ps`

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

### Vérification manuelle

Déclencher un run sans attendre l'échéance cron :

```bash
docker exec -it tradecorp_airflow airflow dags trigger tradecorp_etl_pipeline
```

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
