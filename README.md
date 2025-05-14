# Quantari

Receives Market Data from Binance for a Given Symbol,
and process the information base on multiple strategy that result in order execution

```mermaid
architecture-beta
    service db(database)[TimescaleDB]
    service kafka(database)[Kafka]
    service market(internet)[Market Data]
    service exchange(internet)[Exchange API]
    service oms(server)[Order Management System]
    service tau(server)[Tech Analysis Unit]
    service dpu(server)[Data Process Unit]
    service sms(server)[Strategies Management System]
    junction junctionKafkaBottom
    junction junctionKafkaRight
    junction junctionKafkaSpace

    kafka:B -- T:junctionKafkaBottom
    junctionKafkaBottom:R -- L:junctionKafkaRight
    junctionKafkaRight:R -- L:junctionKafkaSpace
    oms:T -- R:db
    oms:R -- L:exchange
    oms:B -- B:junctionKafkaSpace
    kafka:R -- L:tau
    tau:T -- B:db
    kafka:L -- R:sms
    dpu:R -- L:db
    kafka:T -- B:dpu
    market:R -- L:dpu
```

## 📌 Disclaimer

This software was made for educational purpose and it's not tested with live data

There is no guarantee that this software will work flawlessly at this or later
times. Of course, no responsibility is taken for possible profits or losses.
This software probably has some errors in it, so use it at your own risk. Also
no one should be motivated or tempted to invest assets in speculative forms of
investment. By using this software you release the author(s) from any liability
regarding the use of this software.
