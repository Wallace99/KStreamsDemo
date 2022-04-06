package org.example

import com.fasterxml.jackson.core.JsonProcessingException
import com.fasterxml.jackson.databind.DeserializationFeature
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import org.apache.kafka.common.serialization.Deserializer
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.common.serialization.Serializer
import org.apache.kafka.streams.KafkaStreams
import org.apache.kafka.streams.KeyValue
import org.apache.kafka.streams.StreamsBuilder
import org.apache.kafka.streams.StreamsConfig
import org.apache.kafka.streams.Topology
import org.apache.kafka.streams.kstream.Consumed
import org.apache.kafka.streams.kstream.Produced
import org.apache.kafka.streams.kstream.ValueTransformerWithKey
import org.apache.kafka.streams.processor.ProcessorContext
import org.apache.kafka.streams.state.KeyValueStore
import org.apache.kafka.streams.state.Stores
import org.apache.logging.log4j.LogManager
import org.apache.logging.log4j.Logger
import java.time.Duration
import java.time.Instant
import java.time.temporal.ChronoUnit
import java.util.Properties
import java.util.concurrent.CountDownLatch
import kotlin.system.exitProcess

class KStreamsApplication {

    val logger: Logger = LogManager.getLogger(KafkaStreams::class.java)

    fun start() {
        val props = Properties()
        props[StreamsConfig.APPLICATION_ID_CONFIG] = "kstreams-application";
        props[StreamsConfig.BOOTSTRAP_SERVERS_CONFIG] = "localhost:9092";
        props[StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG] = Serdes.String()::class.java;
        props[StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG] = Serdes.String()::class.java;

        val topology = createTopology()
        val kafkaStreams = KafkaStreams(topology, props)
        val countDownLatch = CountDownLatch(1)

        Runtime.getRuntime().addShutdownHook(object: Thread() {
            override fun run() {
                logger.info("Closing down gracefully")
                kafkaStreams.close()
                countDownLatch.countDown()
            }
        })

        try {
            logger.info("Starting KStreams app")
            kafkaStreams.start()
            countDownLatch.await()
        } catch (e: Throwable) {
            exitProcess(1)
        }
        exitProcess(0)
    }


    private fun createTopology() : Topology {
        val streamBuilder = StreamsBuilder()

        val inMemoryStore = Stores.keyValueStoreBuilder(Stores.inMemoryKeyValueStore("in-memory-store"), Serdes.String(), Serdes.serdeFrom(CustomerSerialiser(), CustomerDeserialiser()))
        streamBuilder.addStateStore(inMemoryStore)

        streamBuilder.stream("customers-events", Consumed.with(Serdes.Long(), Serdes.serdeFrom(CustomerSerialiser(), CustomerDeserialiser())))
            .filterNot { _, value ->  value == null }
            .toTable()
            .transformValues({ CustomValueTransformer() }, "in-memory-store")
            .toStream()
            .filter { _, value -> value.prevTimestamp != null && value.prevTimestamp.isBefore(value.prevTimestamp.minus(5, ChronoUnit.DAYS)) }
            .map { _, value -> KeyValue(value.id, CustomerActive(value.id, Duration.between(value.prevTimestamp, value.prevTimestamp).toDays(), value.timestamp)) }
            .to("active-customers", Produced.with(Serdes.Long(), Serdes.serdeFrom(CustomerActiveSerialiser(), CustomerActiveDeserialiser())))

        return streamBuilder.build()
    }


    inner class CustomValueTransformer : ValueTransformerWithKey<Long, CustomerEvent, CustomerLastSeen> {
        lateinit var state: KeyValueStore<Long, CustomerEvent>

        override fun init(context: ProcessorContext) {
            val stateStore: KeyValueStore<Long, CustomerEvent> = context.getStateStore("in-memory-store")
            state = stateStore
        }

        override fun transform(readOnlyKey: Long?, value: CustomerEvent): CustomerLastSeen {
            val prevValue = state.get(readOnlyKey)
            state.put(readOnlyKey, value)

            if (prevValue == null) {
                return CustomerLastSeen(value.id, value.timestamp, null)
            }

            return CustomerLastSeen(value.id, value.timestamp, prevValue.timestamp)
        }

        override fun close() {}

    }


    inner class CustomerSerialiser : Serializer<CustomerEvent> {

        override fun serialize(topic: String?, customerEvent: CustomerEvent): ByteArray? {
            return try {
                jacksonObjectMapper().registerModule(JavaTimeModule()).writeValueAsBytes(customerEvent)
            } catch (e: JsonProcessingException) {
                logger.error("Unable to serialise active customer {}", customerEvent, e)
                null
            }
        }

    }


    inner class CustomerDeserialiser : Deserializer<CustomerEvent> {

        override fun deserialize(topic: String?, customerEvent: ByteArray?): CustomerEvent? {
            return try {
                jacksonObjectMapper().configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false).registerModule(JavaTimeModule()).readerFor(CustomerEvent::class.java).readValue(customerEvent)
            } catch (e: JsonProcessingException) {
                logger.error("Unable to serialise active customer {}", customerEvent, e)
                null
            }
        }

    }


    inner class CustomerActiveSerialiser : Serializer<CustomerActive> {

        override fun serialize(topic: String?, customerActive: CustomerActive): ByteArray? {
            return try {
                jacksonObjectMapper().registerModule(JavaTimeModule()).writeValueAsBytes(customerActive)
            } catch (e: JsonProcessingException) {
                logger.error("Unable to serialise active customer {}", customerActive, e)
                null
            }
        }

    }


    inner class CustomerActiveDeserialiser : Deserializer<CustomerActive> {

        override fun deserialize(topic: String?, customerActive: ByteArray?): CustomerActive? {
            return try {
                jacksonObjectMapper().configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false).registerModule(JavaTimeModule()).readerFor(CustomerEvent::class.java).readValue<CustomerActive>(customerActive)
            } catch (e: JsonProcessingException) {
                logger.error("Unable to serialise active customer {}", customerActive, e)
                null
            }
        }

    }


    data class CustomerEvent(val id: Long, val timestamp: Instant)

    data class CustomerLastSeen(val id: Long, val timestamp: Instant, val prevTimestamp: Instant?)

    data class CustomerActive(val id: Long, val daysSinceActive: Long, val timestamp: Instant)
}


fun main() {
    val app = KStreamsApplication()
    app.start()
}