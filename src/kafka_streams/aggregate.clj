(ns kafka-streams.aggregate
  (:require [kafka-streams.core :as ks])
  (:import [java.util Properties]
           [kafka.streams.clojure ContextAwareProcessor]
           [org.apache.kafka.common.serialization Serdes]
           [org.apache.kafka.clients.consumer ConsumerConfig]
           [org.apache.kafka.streams KafkaStreams StreamsConfig]
           [org.apache.kafka.streams.processor TopologyBuilder ProcessorSupplier]
           [org.apache.kafka.streams.state Stores]
           [org.apache.kafka.streams.kstream KStreamBuilder]))

(defn handle-event [event agg]
  (condp = event
      "added" (inc agg)
      "subtracted" (dec agg)))

(defn process [key event context]
  (let [store (.getStateStore context "Aggregates")
        agg (or (.get store key) 0)
        new-agg (handle-event event agg)]
    (.put store key new-agg)
    (.forward context key new-agg)))

(def processor-supplier (reify ProcessorSupplier
                          (get [_]
                            (ContextAwareProcessor. process identity identity))))

(def state-store-supplier
  (-> (Stores/create "Aggregates")
      (.withKeys (Serdes/Long))
      (.withValues (Serdes/Long))
      (.persistent)
      (.build)))

;; Apply a stream of events to an aggregate and write the new changes to kafka.
(defn aggregate-example [broker-connect zoo-keeper-connect]
  (let [props (Properties.)
        builder (TopologyBuilder.)
        long-serde (Serdes/Long)
        string-serde (Serdes/String)]

    (doto props
      (.put StreamsConfig/APPLICATION_ID_CONFIG "aggregate-example")
      (.put ConsumerConfig/AUTO_OFFSET_RESET_CONFIG "earliest")
      (.put StreamsConfig/BOOTSTRAP_SERVERS_CONFIG broker-connect)
      (.put StreamsConfig/ZOOKEEPER_CONNECT_CONFIG zoo-keeper-connect)
      (.put StreamsConfig/STATE_DIR_CONFIG "/tmp/kafka-streams"))

    (-> builder
        (.addSource "events" (.deserializer long-serde) (.deserializer string-serde) (into-array ["events-topic"]))
        (.addProcessor "Aggregate" processor-supplier (into-array ["events"]))
        (.addStateStore state-store-supplier (into-array ["Aggregate"]))
        (.addSink "aggregates" "aggregates-topic" (.serializer long-serde) (.serializer long-serde) (into-array ["Aggregate"])))
    
    (KafkaStreams. builder props)))
