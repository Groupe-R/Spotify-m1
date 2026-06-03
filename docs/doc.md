# Validation Issue #11 - Kafka KRaft Cluster

## Brokers

![Brokers](docs/screenshots/kafka-brokers.png)

## Topics

![Topics](docs/screenshots/kafka-topics.png)

## Listening_events

![Listening Events](docs/screenshots/kafka-listening-events.png)

## Catalog_updates

![Catalog_updates](docs/screenshots/kafka-Catalog_updates.png)

## Conclusion

Le cluster Kafka KRaft est opérationnel et conforme aux critères de validation de l'Issue #11.


# Validation Issue #12 - Migration Simulateur P2P vers Kafka

## Messages Kafka

Les événements sont publiés en continu dans le topic `listening_events`.

![Kafka Messages](docs/screenshots/kafka-messages.png)

## Compatibilité Phase 1

Les DAGs Airflow Phase 1 restent opérationnels après l'ajout de Kafka.


## Conclusion

- Publication simultanée Redis + Kafka
- Messages visibles dans Kafka UI
- DAGs Phase 1 toujours fonctionnels

Issue #12 validée.

## Validation Issue #13 - Spark Kafka Console Reader

Le cluster Spark est opérationnel avec un master et un worker actif.

![Spark Master](docs/screenshots/spark-master.png)

Le job Spark lit le topic Kafka `listening_events` et affiche les événements en console.

![Spark Console](docs/screenshots/spark-console-events.png)

Validation :
- Spark Master démarré
- Spark Worker connecté
- Lecture du topic Kafka `listening_events`
- Événements JSON visibles dans les logs Spark