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
