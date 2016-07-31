(ns kafka-streams.test-utils

  "Testing utilities for embedded Kafka and Zookeeper instances. Followes the KafkaEmbedded class in the kafka-streams project."

  (:require [com.stuartsierra.component :as component]
            [clojure.tools.logging :as log])
  (:import [java.nio.file Files SimpleFileVisitor FileVisitResult]
           [org.apache.kafka.common.protocol SecurityProtocol]
           [org.apache.curator.test TestingServer]
           [kafka.utils TestUtils SystemTime$]
           [kafka.server KafkaConfig KafkaConfig$]))

(defn delete-dir [path-dir]
  (Files/walkFileTree path-dir (proxy [SimpleFileVisitor] []
                                 (visitFile [path _]
                                   (Files/delete path)
                                   FileVisitResult/CONTINUE)
                                 (postVisitDirectory [dir _]
                                   (Files/delete dir)
                                   FileVisitResult/CONTINUE))))

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
    (let [log-dir-path (Files/createTempDirectory "kafka-embedded" (into-array java.nio.file.attribute.FileAttribute []))
          effective-config (-> default-config
                               (merge init-config)
                               (assoc "zookeeper.connect" (.getConnectString (:server zoo-keeper)))

                               (assoc (.LogDirProp KafkaConfig$/MODULE$) (str log-dir-path)))]
      (-> component
          (assoc :log-dir-path log-dir-path)
          (assoc :server (TestUtils/createServer (KafkaConfig. effective-config true) SystemTime$/MODULE$)))))
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
  (let [server (get-in cluster [:kafka-broker :server])
        host-name (.. server config hostName)
        port (.boundPort server SecurityProtocol/PLAINTEXT)]
    (str host-name ":" port)))

(defn zk-connect-string [cluster]
  (let [server (get-in cluster [:zoo-keeper :server])]
    (.getConnectString server)))


