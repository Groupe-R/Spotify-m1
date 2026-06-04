# Validation Issue #11 - Kafka KRaft Cluster

## Brokers

![Brokers](screenshots/kafka-brokers.png)

## Topics

![Topics](screenshots/kafka-topics.png)

## Listening_events

![Listening Events](screenshots/kafka-listening-events.png)

## Catalog_updates

![Catalog_updates](screenshots/Kafka-catalog-updates.png)

## Conclusion

Le cluster Kafka KRaft est opérationnel et conforme aux critères de validation de l'Issue #11.


# Validation Issue #12 - Migration Simulateur P2P vers Kafka

## Messages Kafka

Les événements sont publiés en continu dans le topic `listening_events`.

![Kafka Messages](screenshots/kafka-messages.png)

## Compatibilité Phase 1

Les DAGs Airflow Phase 1 restent opérationnels après l'ajout de Kafka.


## Conclusion

- Publication simultanée Redis + Kafka
- Messages visibles dans Kafka UI
- DAGs Phase 1 toujours fonctionnels

Issue #12 validée.

# Validation Issue #13 - Spark Kafka Console Reader
 
Le cluster Spark est opérationnel avec un master et un worker actif.

![Spark Master](screenshots/spark-master.png)

Le job Spark lit le topic Kafka `listening_events` et affiche les événements en console.

![Spark Console](screenshots/spark-console-events.png)

Validation :
- Spark Master démarré
- Spark Worker connecté
- Lecture du topic Kafka `listening_events`
- Événements JSON visibles dans les logs Spark

# Validation Issue #14 - Streaming Aggregations avec Fenêtres Temporelles

## Agrégation Top Tracks (Tumbling Window)

Le job Spark calcule les morceaux les plus écoutés à partir du flux Kafka `listening_events` en utilisant une fenêtre temporelle tumbling de 5 minutes.

Les métriques calculées sont :
- `stream_count`
- `unique_listeners`

## Écriture PostgreSQL

Les résultats des agrégations sont écrits automatiquement dans la table PostgreSQL `realtime_top_tracks` via `foreachBatch`.

![Realtime Top Tracks](screenshots/realtime-top-tracks.png)

## Exécution Spark

Le job Spark traite les micro-batches en continu et écrit les résultats dans PostgreSQL.

![Spark Aggregation](screenshots/spark-aggregation-batches.png)

## Validation

- Agrégation Spark Streaming opérationnelle
- Fenêtre tumbling de 5 minutes implémentée
- Calcul des `stream_count`
- Calcul des `unique_listeners`
- Écriture PostgreSQL via `foreachBatch`
- Table `realtime_top_tracks` alimentée automatiquement

## Conclusion

La pipeline d'agrégation streaming est opérationnelle et conforme aux critères de validation de l'Issue #14.

# Validation Issue #15 - Watermarking et gestion des Late Events

## Watermark Spark

Le watermark de 10 minutes est configuré sur le champ `event_time`.

![Spark Watermark](screenshots/spark-watermark.png)

## Génération des Late Events

Le simulateur a été exécuté en mode `late_events` avec cette commande :
```bash
python -m src.p2p_simulator.simulator --mode late_events
``` 

![Late Events Simulator](screenshots/late-events-simulator.png)

## Validation Kafka

Les événements tardifs sont correctement routés vers le topic `late_listening_events`.
```bash
docker exec -it spotify-m1-kafka-1-1 kafka-console-consumer \
  --bootstrap-server kafka-1:9092 \
  --topic late_listening_events \
  --from-beginning \
  --max-messages 5
```

![Kafka Late Events](screenshots/kafka-late-events.png)

## Exécution Spark

Le cluster Spark reste opérationnel pendant le traitement des événements tardifs.

![Spark Master](screenshots/spark-master.png)

## Validation

- Watermark de 10 minutes configuré
- Mode `late_events` activé
- Détection des événements tardifs
- Routage vers `late_listening_events`
- Vérification des messages dans Kafka
- Traitement Spark Streaming opérationnel

## Conclusion

La gestion des événements tardifs est opérationnelle et conforme aux critères de validation de l'Issue #15.

# Validation Issue #16 - Exactly Once Processing

## Configuration Kafka Producer

Le simulateur Kafka est configuré avec l'idempotence activée afin d'éviter la publication de doublons.

Paramètres utilisés :

- `enable.idempotence = true`
- `acks = all`
- `transactional.id = p2p-simulator-1`

![Kafka Producer Idempotence](screenshots/kafka-producer-idempotence.png)

## Configuration Spark Consumer

Le consommateur Spark lit uniquement les messages validés grâce au niveau d'isolation `read_committed`.

![Spark Read Committed](screenshots/spark-read-committed.png)

## Test de redémarrage du job Spark

Le job Spark a été démarré, arrêté puis relancé afin de vérifier que le traitement reprend correctement sans générer de doublons.

![Spark Restart](screenshots/spark-restart.png)

## Vérification PostgreSQL

La vérification des doublons a été effectuée avec la requête suivante :

```sql
SELECT COUNT(*) - COUNT(DISTINCT id) AS doublons
FROM listening_events;
```

Résultat obtenu :

```text
0
```

![PostgreSQL Duplicates Check](screenshots/postgres-duplicates-check.png)

## Validation

- Producer Kafka idempotent configuré
- Consumer Spark configuré en mode `read_committed`
- Arrêt et redémarrage du job Spark validés
- Aucun doublon détecté dans PostgreSQL

## Conclusion

La chaîne Kafka → Spark → PostgreSQL respecte le principe d'Exactly Once Processing.

L'Issue #16 est validée.

# Validation Issue #17 - Streaming Enrichment Job

## Enrichissement des événements

Le job `streaming_enrichment_job.py` lit les événements depuis Kafka (`listening_events`) et les enrichit avec le catalogue PostgreSQL (`tracks` et `artists`).

Les événements sont enrichis avec :
- `track_title`
- `artist_name`
- `genre`
- `artist_country`

## Jointure avec les événements P2P

Le job réalise également une jointure stream-stream entre `listening_events` et `p2p_network_events`, avec un watermark de 2 minutes.

Les champs P2P ajoutés sont :
- `p2p_event_type`
- `p2p_peer_id`
- `p2p_latency_ms`

![Spark Enrichment Output](screenshots/spark-enrichment-output.png)

## Déduplication

Une déduplication est appliquée sur `event_id` afin d'éviter les doublons dans le flux enrichi.

![Deduplication Code](screenshots/deduplication-code.png)

## Écriture Kafka

Les événements enrichis sont publiés dans le topic Kafka `enriched_events`.

![Kafka Enriched Events](screenshots/kafka-enriched-events.png)

## Écriture MinIO Parquet

Les événements enrichis sont également écrits au format Parquet dans MinIO, partitionnés par `date` et `hour`.

![MinIO Enriched Parquet](screenshots/minio-enriched-parquet.png)
![MinIO Enriched Parquet](screenshots/minio-enriched-parquet2.png)
## Validation

- Lecture du topic Kafka `listening_events`
- Chargement du catalogue PostgreSQL
- Jointure stream-static avec `tracks` et `artists`
- Jointure stream-stream avec `p2p_network_events`
- Watermark de 2 minutes
- Déduplication par `event_id`
- Écriture dans Kafka `enriched_events`
- Écriture Parquet dans MinIO partitionnée par `date/hour`

## Conclusion

Le job `streaming_enrichment_job.py` est opérationnel et conforme aux critères de validation de l'Issue #17.
<<<<<<< HEAD
=======

# Validation Issue #18 - Fraud Detection Job

## Détection de fraude en temps réel

Le job `fraud_detection_job.py` consomme les événements depuis Kafka :

- `listening_events`
- `p2p_network_events`

Le traitement est réalisé avec Spark Structured Streaming afin de détecter les comportements frauduleux en temps réel.

![Spark Fraud Detection Output](screenshots/fraud-detection-output.png)

## Règle 1 — Burst Listening

Le job détecte les utilisateurs ayant un nombre anormalement élevé d’écoutes sur une fenêtre de 10 minutes.

Type d’alerte généré :

- `burst_listening`

![Burst Listening](screenshots/burst-listening.png)

## Règle 2 — Short Duration Bot

Le job détecte les utilisateurs ayant plusieurs écoutes très courtes, avec une durée moyenne inférieure à 5 secondes sur une fenêtre d’une heure.

Type d’alerte généré :

- `short_duration_bot`


![Short Duration Bot Alert](screenshots/short-duration-bot-alert.png)

## Règle 3 — P2P Failure Rate

Le job analyse les événements P2P et détecte les peers ayant un taux d’échec de transfert supérieur à 50 % sur une fenêtre de 15 minutes.

Type d’alerte généré :

- `p2p_failure_rate`

## Publication Kafka

Les alertes détectées sont publiées dans le topic Kafka :

- `fraud_alerts`

![Kafka Fraud Alerts](screenshots/kafka-fraud-alerts.png)

## Écriture PostgreSQL

Les alertes sont enregistrées dans PostgreSQL avec les champs suivants :

- `user_id`
- `peer_id`
- `fraud_type`
- `suspicion_score`
- `evidence`
- `window_start`
- `window_end`
- `detected_at`

![PostgreSQL Fraud Detections](screenshots/postgres-fraud-detections.png)

## Validation

- Activation du simulateur en mode fraud
- Lecture des topics Kafka `listening_events` et `p2p_network_events`
- Détection des fraudes `burst_listening`
- Détection des fraudes `short_duration_bot`
- Détection des fraudes `p2p_failure_rate`
- Publication des alertes dans Kafka `fraud_alerts`
- Enregistrement des alertes dans PostgreSQL

## Conclusion

Le job `fraud_detection_job.py` est opérationnel et conforme aux critères de validation de l’Issue #18.

Les alertes de fraude sont correctement détectées en temps réel, publiées dans Kafka et persistées dans PostgreSQL.
