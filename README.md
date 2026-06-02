# Spotify Data Platform — Phase 1

Plateforme de data engineering simulant un service de streaming musical avec ingestion catalogue, événements d’écoute, agrégations, recommandations et retraitement DLQ.

## Architecture Phase 1

```text
Data Generator → MinIO → Airflow DAGs → PostgreSQL
P2P Simulator → Redis → streaming_events_pipeline → PostgreSQL / MinIO
PostgreSQL → aggregation_pipeline → daily_streams / artist_stats / realtime_top_tracks
PostgreSQL → recommendation_pipeline → Redis / recommendations
dead_letter_events → dlq_reprocessing_pipeline → listening_events / status update
```

## Fonctionnalités implémentées

- Setup Docker Compose
- Schéma PostgreSQL complet
- Générateur de données Faker
- DAG catalog_ingestion_pipeline
- Simulateur P2P avec Redis
- DAG streaming_events_pipeline
- DAG aggregation_pipeline
- DAG recommendation_pipeline
- DAG dlq_reprocessing_pipeline
- Tests unitaires et tests de structure DAGs
- Documentation doc_md sur les DAGs

## Lancer le projet
```Bash
docker compose up -d
```

Interfaces: 

- Airflow : http://localhost:8080
- MinIO   : http://localhost:9001
- Postgres: localhost:5432
- Redis   : localhost:6379

## Lancer le simulateur P2P
```Bash
python3 src/p2p_simulator/simulator.py
```

## Lancer les tests

Tests unitaires :
```Bash
python3 -m pytest tests/unit/test_transformations.py -v
```
Tests de structure Airflow, à lancer dans le conteneur Airflow :
```Bash
docker exec -it spotify-m1-airflow-worker-1 bash

pytest tests/structure/test_dag_structure.py -v
```

Résultats obtenus :
- 18 passed: tests unitaires
- 16 passed: tests structure DAGs16 passed — tests structure DAGs

## DAGs Phase 1

- catalog_ingestion_pipeline
- streaming_events_pipeline
- aggregation_pipeline
- recommendation_pipeline
- dlq_reprocessing_pipeline

## Validation Phase 1

- Les 5 DAGs sont importés par Airflow.
- Le simulateur P2P publie des événements dans Redis.
- Les événements valides sont stockés dans PostgreSQL et MinIO.
- Les événements invalides sont routés vers la DLQ.
- Les agrégations et recommandations sont générées.
- La DLQ est retraitée avec gestion des statuts pending, reprocessed et abandoned.
