# AnalisiAutovelox
Analisi Real Time di dati provenienti da autovelox tramite la libreria Structured Streaming di PySpark.

**Tool Utilizzati**

    - Apache PySpark (elaborazione in batch dei dati)
    - Apache Kafka (per simulare l'arrivo real time dei dati)
    - Apache Druid (Real time database)
    - Apache Superset (Data visualization)

Riepilogo degli step eseguiti nel progetto:

**DATA CLEANING**

    1) delete_records.sh (faccio in modo che gli indici 1,2,3,...,9 diventino 01,02,03,...,09(padding). Poi da 10 a 31 niente padding)
    2) Elimino i giorni relativi al giorno successivo rispetto al file considerato (sempre delete_records.sh)
    3) con clear_date_all.sh levo la data (anno-mese-giorno) dalla colonna timestamp, così ci lasciamo solo ore-minuti-secondi
    4) Faccio il sort dei record per ogni file del mese (sort.py)
    5) delete_records.sh (elimina i giorni che non fanno riferimento al giorno del file) -> sort.py (by timestamp) -> clear_date_all.sh

**INIZIALIZZAZIONE KAFKA**

    1) bin/zookeeper-server-start.sh config/zookeeper.properties (start_zookeper.sh)
    2) bin/kafka-server-start.sh config/server.properties (start_kafka_server.sh)

**CREATE TOPIC** 

    1) create_topic.sh

**CREA IL PRODUCER (scrive eventi sul topic)**

    1) my_producer.sh (che utilizza write_events.sh)

**ELABORAZIONE DEI TRATTI AUTOSTRADALI A PARTIRE DAL PDF**

    1) tratto, ingresso, uscita
    2) tratto, ingresso, uscita, lunghezza

**PREPARAZIONE AMBIENTE**

    1) Ambiente virtuale conda
    2) Installazione PySpark

**ANALISI PYSPARK (<programma --> output topic>)**

    0) my_producer.sh --> autovelox
    1) execute.sh (last_seen.py) --> ultimi-avvistamenti
    2) execute2.sh (vehicle_count.py) --> conteggio_veicoli
    3) execute3.sh (df_aggregated.py) --> df_aggregated
    4) execute4.sh (df_computed.py) --> dd1
    5) execute5.sh (kmeans.py) --> kMeans

**CATENA DIPENDENZE TOPIC**

    1) autovelox --> ultimi-avvistamenti --> conteggio_veicoli
    2) autovelox --> df_aggregated --> df_computed --> kMeans

