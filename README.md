# 🚀 TradeCorp International - Pipeline Data ETL Industrialisé

![Docker](https://img.shields.io/badge/Docker-Compose-blue?logo=docker)
![PySpark](https://img.shields.io/badge/PySpark-3.5-orange?logo=apachespark)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-16-blue?logo=postgresql)
![Apache Airflow](https://img.shields.io/badge/Apache%20Airflow-2.8-teal?logo=apacheairflow)
![Azure ADLS Gen2](https://img.shields.io/badge/Azure-ADLS%20Gen2-0078D4?logo=microsoftazure)

---

## 📌 Contexte & Enjeux Métier
Chaque nuit, **TradeCorp International** reçoit ses données commerciales sous forme de fichiers CSV bruts. Le traitement était jusqu'à présent réalisé manuellement chaque matin (environ 3 heures de traitement), générant des erreurs, un manque d'historisation et une absence totale de traçabilité.

**Objectif du projet :** Concevoir, conteneuriser et automatiser un pipeline Data ETL robuste, évolutif et sécurisé pour transformer les flux bruts en données enrichies exploitables par les équipes métiers et les décideurs.

---

## 🏗️ Architecture Technique Target

```
[ Sources CSV Brutes ] ──> [ Azure ADLS Gen2 (raw/) ]
                                   │
                                   ▼
 [ API Externe ] ─────────> [ Apache Spark ] <── [ Azure Key Vault (Secrets) ]
                                   │
                  ┌────────────────┴────────────────┐
                  ▼                                 ▼
   [ Azure ADLS Gen2 (clean/) ]          [ PostgreSQL ]
      (Fichiers Parquet)               (Table orders_enriched)
                  │                                 │
                  └────────────────┬────────────────┘
                                   │
                          [ Apache Airflow ]
                      (Orchestration globale DAG)
```
---

## 🛠️ Stack Technique & Justifications

* **Docker & Dev Containers** : Garantit la reproductibilité exacte de l'environnement de développement et de production.
* **Apache Spark (PySpark)** : Moteur de calcul distribué pour le traitement à grande échelle, l'enrichissement et les agrégations complexes (**Window Functions**).
* **Format Parquet & Partitionnement** : Format de stockage en colonnes hautement compressé. Partitionnement par pays (`partitionBy('country')`) pour optimiser le *Partition Pruning*.
* **PostgreSQL (Driver JDBC `42.7.0`)** : Entrepôt de données relationnel pour l'exposition des KPI métiers.
* **Azure ADLS Gen2 & Key Vault** : Stockage Cloud Data Lake centralisé (zones `raw` / `clean`) et gestion ultra-sécurisée des secrets.
* **Apache Airflow** : Orchestration automatisée, gestion des dépendances et suivi des exécutions.
* **Pytest** : Validation de la qualité du code via des tests unitaires automatisés.

---

## 📂 Structure du Dépôt

```
brief_spark/
├── .gitignore                  # Exclusion des données brutes, secrets et caches
├── README.md                   # Documentation principale du projet
├── docker-compose.yml          # Définition des services Spark et Postgres
├── data/                       # Données brutes CSV et exports (ignoré par Git)
└── notebooks/                  # Phase d'exploration et prototypage
    ├── 01_exploration.ipynb    # Ingestion initiale et analyse de schéma
    ├── 02_nettoyage.ipynb      # Traitement des nuls, jointures et filtres
    └── 03_transformations.ipynb# Window Functions (cumul) et export JDBC/Parquet
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