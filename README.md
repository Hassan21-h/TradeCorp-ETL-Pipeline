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

**Objectif du projet :** Industrialiser un pipeline ETL PySpark conteneurisé qui télécharge les fichiers métiers et référentiels depuis Azure ADLS Gen2, applique des transformations complexes (nettoyage, calculs financiers, conversion dynamique de devises), persiste les données enrichies en format Parquet et garantit la qualité via une suite de tests unitaires automatisés.

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
├── docker-compose.yml         # Orchestration des services
├── requirements.txt           # Dépendances Python (pyspark, azure-storage-blob, pytest)
├── data/                      # Données locales brutes et référentiels
│   └── reference/             # Referentiels locaux (country_currency.csv, exchange_rates.json)
├── notebooks/                 # Exploration et prototypage
├── src/                       # Code source modulaire du pipeline
│   ├── reader.py              # Ingestion Azure ADLS & création SparkSession
│   ├── transformer.py         # Métrique métier et calculs sous-totaux
│   ├── enrichment.py          # Logique d'enrichissement devises & conversion
│   ├── writer.py              # Export Parquet & Upload Azure ADLS
│   ├── utils.py               # Utilitaires de gestion du Data Lake
│   └── pipeline.py            # Script d'exécution principal du pipeline ETL
└── tests/                     # Suite de tests unitaires automatisés
├── run_tests.py               # Runner de tests
└── test_transformers.py       # Tests unitaires PySpark (mock DataFrame & devises)
```
---

## ⚡ Prise en Main Rapide

### 1. Prérequis
* Docker Desktop installé et démarré.
* Git configuré sur votre machine.

### 2. Démarrage de l'infrastructure
* Lancer les conteneurs : `docker-compose up -d`
* Vérifier le statut : `docker compose ps`

### 3. Accès aux interfaces web
* **JupyterLab :** `http://localhost:8888`
* **Spark UI :** `http://localhost:4041`
* **PostgreSQL (DBeaver) :** `localhost:5454` *(User: tradecorp, Database: tradecorp)*

---

## 📋 Suivi de Projet & Méthodologie Agile

Le projet est piloté selon la méthode Kanban répartie sur 3 jalons principaux 

### État d'avancement des Jalons :
- [x] **Jalon 1 :** Cadrage, étude des risques, conteneurisation Docker, exploration des données et ingestion.
- [x] **Jalon 2 :** Transformations PySpark, Window Functions, export JDBC Postgres et écriture Parquet.
- [x] **Jalon 3 :** Orchestration Airflow, modularisation du code, documentation finale et préparation de la soutenance.