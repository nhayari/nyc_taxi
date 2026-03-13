i# NYC Taxi Databricks Pipeline

Projet de **Data Pipeline** end-to-end sur **Databricks** utilisant l'architecture **Medallion** (Bronze / Silver / Gold) pour traiter les données **NYC TLC Trip Records** (Yellow Taxi, Green Taxi, High-Volume For-Hire Vehicles).

Objectif : ingérer, nettoyer, enrichir et agréger les trajets de taxis à New York pour analyses, reporting, BI ou ML (ex. : hotspots de demande, prédiction de pourboires, optimisation de flotte, analyse de congestion).

## Architecture globale (Medallion)

- **Bronze** : Données brutes ingérées telles quelles (Parquet TLC, historique mensuel conservé, pas de nettoyage)
- **Silver** : Données nettoyées, typées, filtrées (outliers supprimés), jointures avec lookup zones, enrichissements (ex. : heure du jour, jour de la semaine, borough)
- **Gold** : Tables analytiques prêtes à l'emploi (agrégations par zone/heure/jour, métriques business comme distance moyenne, revenu par shift, taux de pourboire, etc.)

## Structure du repository

```text
nyc_taxi-databricks-pipeline/
│
├── utils/                  # Fonctions réutilisables (config, helpers Spark, logging...)
│   └── download_utils.py   # Scripts pour télécharger datasets TLC (Parquet mensuels)
│
├── ingestion/              # Notebooks / Jobs d'ingestion (Auto Loader sur S3/URL, COPY INTO...)
│
├── bronze/                 # Notebooks / SQL pour créer & charger la couche Bronze
│
├── silver/                 # Nettoyage, validation, qualité → Silver (incl. zone lookup join)
│
├── gold/                   # Agrégations, jointures analytiques → Gold
│
├── README.md
└── requirements.txt        # (optionnel) Dépendances Python si besoin
