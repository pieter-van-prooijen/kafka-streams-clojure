(ns kafka-streams.test-utils

  "Testing utilities for embedded Kafka and Zookeeper instances. Followes the KafkaEmbedded class in the kafka-streams project."

  (:require [com.stuartsierra.component :as component]
            [clojure.tools.logging :as log])
  (:import [java.nio.file Files Paths SimpleFileVisitor FileVisitResult]
           [org.apache.kafka.common.protocol SecurityProtocol]
           [org.apache.kafka.common.serialization Serdes]
           [org.apache.kafka.common TopicPartition]
           [org.apache.curator.test TestingServer]
           [org.apache.kafka.clients.producer KafkaProducer ProducerRecord]
           [org.apache.kafka.clients.consumer KafkaConsumer ConsumerRecord]
           [kafka.utils TestUtils SystemTime$]
           [kafka.server KafkaConfig KafkaConfig$]))

(defn delete-dir [path-dir]
  (when (Files/isReadable path-dir)
    (Files/walkFileTree path-dir (proxy [SimpleFileVisitor] []
                                   (visitFile [path _]
                                     (Files/delete path)
                                     FileVisitResult/CONTINUE)
                                   (postVisitDirectory [dir _]
                                     (Files/delete dir)
                                     FileVisitResult/CONTINUE)))))

(defrecord EmbeddedZooKeeper [server]
  component/Lifecycle
  (start [component]
    (log/info "starting zookeeper")
    (assoc component :server (TestingServer.)))
  (stop [component]
    (when-let [server (:server component)]
      (.close (:server component))
      (log/info "stopped zookeeper"))
    (assoc component :server nil)))

(def default-config
  (let [m  KafkaConfig$/MODULE$]
    {(.BrokerIdProp m) (int 0)
     (.HostNameProp m) "127.0.0.1"
     (.PortProp m) (int 0)              ; 0 selects a random port
     (.NumPartitionsProp m) (int 1)
     (.AutoCreateTopicsEnableProp m) true
     (.DeleteTopicEnableProp m) true
     (.LogCleanerDedupeBufferSizeProp m) (* 2 1024 1024)
     (.MessageMaxBytesProp m) (int (* 1024 1024)) 
     (.ControlledShutdownEnableProp m) true}))

(defrecord EmbeddedKafka [init-config log-dir-path zoo-keeper]
  component/Lifecycle
  (start [component]
    (log/info "starting kafka broker")
    (delete-dir (Paths/get "/tmp/kafka-streams" (into-array String []))) ; local aggregate store, remove after each test.
    (let [log-dir-path (Files/createTempDirectory "kafka-embedded" (into-array java.nio.file.attribute.FileAttribute []))
          effective-config (-> default-config
                               (merge init-config)
                               (assoc "zookeeper.connect" (.getConnectString (:server zoo-keeper)))

                               (assoc (.LogDirProp KafkaConfig$/MODULE$) (str log-dir-path)))]
      (-> component
          (assoc :log-dir-path log-dir-path)
          (assoc :server (TestUtils/createServer (KafkaConfig. effective-config true) kafka.utils.SystemTime$/MODULE$)))))
  (stop [component]
    (when-let [server (:server component)]
      (.shutdown server)
      (.awaitShutdown server)
      (log/info "stopped kafka broker"))
    (when-let [dir (:log-dir-path component)]
      (delete-dir dir))
    (assoc component :server nil :log-dir-path nil)))

(defn new-kafka-cluster [config]
  (component/system-map
   :kafka-broker (component/using (map->EmbeddedKafka config) [:zoo-keeper])
   :zoo-keeper (map->EmbeddedZooKeeper config)))

(defn broker-connect-string [cluster]
  "Answer the broker connect string for the specified cluster system map."
  (let [server (get-in cluster [:kafka-broker :server])
        host-name (.. server config hostName)
        port (.boundPort server SecurityProtocol/PLAINTEXT)]
    (str host-name ":" port)))

(defn zoo-keeper-connect-string [cluster]
  "Answer the zoo-keeper connect string for the specified cluster system map."
  (let [server (get-in cluster [:zoo-keeper :server])]
    (.getConnectString server)))

;;
;; Cluster fixture variables and setup
;;
(declare ^:dynamic *cluster*)
(declare ^:dynamic *broker-connect*)
(declare ^:dynamic *zoo-keeper-connect*)

(defn cluster-fixture [test]
  (let [cluster (new-kafka-cluster {})
        cluster (component/start cluster)]
    (with-redefs [*cluster* cluster
                  *broker-connect* (broker-connect-string cluster)
                  *zoo-keeper-connect* (zoo-keeper-connect-string cluster)]
      (try 
        (test)
        (finally (component/stop cluster))))))

;; Default properties for test producer / consumer
(def default-serde (Serdes/String))
(def default-props {})

(def default-props-producer {"key.serializer" (.. default-serde serializer getClass getName)
                             "value.serializer" (.. default-serde serializer getClass getName)
                             "acks" "all"
                             "retries" (int 0)})

(def default-props-consumer {"key.deserializer" (.. default-serde deserializer getClass getName) 
                             "value.deserializer" (.. default-serde deserializer getClass getName)
                             "auto.offset.reset" "earliest"})

(defn produce-key-values-sync [topic kvs broker-connect extra-props]
  "Put the supplied values (with nil keys) as strings on the specified topic and wait for confirmation.
   Auto-create topics should be on in the cluster"
  (let [props (merge default-props default-props-producer {"bootstrap.servers" broker-connect} extra-props)
        producer (KafkaProducer. props)]
    (doseq [[k v] kvs]
      (let [record (ProducerRecord. topic k v)
            f (.send producer record)]
        (.get f)))
    (.flush producer)
    (.close producer)))

(defn produce-values-sync [topic vs broker-connect props]
  (produce-key-values-sync topic (map (fn [x] [nil x]) vs) broker-connect props))
    
(defn from-consumer-record [record]
  [(.key record) (.value record)])

(defn consume-key-values [topic nof-items broker-connect extra-props]
  "Read at least nof-items key-value pairs from the specified topic, starting at beginning of the log."
  (let [props (merge default-props default-props-consumer
                     {"group.id" "kafka-streams.test-utils" "bootstrap.servers" broker-connect}
                     extra-props)
        consumer (KafkaConsumer. props)]
    (.subscribe consumer [topic])
    (log/debug "Polling for key-values...")
    (let [consumer-records  (.poll consumer 3000) ; time-out when retrieving values
          key-values (->> consumer-records
                          (.iterator)
                          (iterator-seq)
                          (take nof-items)
                          (map from-consumer-record)
                          (doall))]
      (log/debug "after poll")
      (.close consumer)
      key-values)))
