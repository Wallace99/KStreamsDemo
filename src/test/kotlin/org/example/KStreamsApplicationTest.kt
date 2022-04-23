package org.example

import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.StreamsConfig
import org.apache.kafka.streams.TestInputTopic
import org.apache.kafka.streams.TestOutputTopic
import org.apache.kafka.streams.Topology
import org.apache.kafka.streams.TopologyTestDriver
import org.apache.kafka.streams.state.KeyValueStore
import org.assertj.core.api.Assertions.assertThat
import org.example.KStreamsApplication.CustomerEvent
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import java.time.Instant
import java.util.*
import kotlin.math.exp

internal class KStreamsApplicationTest {

    private lateinit var topology: Topology
    private lateinit var application: KStreamsApplication
    private lateinit var driver: TopologyTestDriver
    private lateinit var inputTopic: TestInputTopic<Long, CustomerEvent>
    private lateinit var outputTopic: TestOutputTopic<Long, KStreamsApplication.CustomerActive>
    private lateinit var store: KeyValueStore<Long, CustomerEvent>

    private val customerId = 123L

    @BeforeEach
    fun setup() {
        val props = Properties()
        props.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234")
        props.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, "test")

        application = KStreamsApplication()
        topology = application.createTopology()
        driver = TopologyTestDriver(topology, props)

        inputTopic = driver.createInputTopic("customer-events", Serdes.Long().serializer(), application.CustomerSerialiser())
        outputTopic = driver.createOutputTopic("active-customers", Serdes.Long().deserializer(), application.CustomerActiveDeserialiser())

        store = driver.getKeyValueStore("customer-store")
    }


    @AfterEach
    fun tearDown() {
        driver.close()
    }


    @Test
    fun `test given empty state when receive event then add to state store`() {
        val instant = Instant.parse("2022-04-19T14:59:00Z")
        val value = CustomerEvent(customerId, instant)
        inputTopic.pipeInput(customerId, value)

        val savedValue = store.get(customerId)
        assertThat(savedValue.timestamp).isEqualTo(instant)
    }


    @Test
    fun `test given customer existing in state store when receive event then overwrite timestamp`() {
        val instant = Instant.parse("2022-04-19T14:59:00Z")
        val value = CustomerEvent(customerId, instant)
        inputTopic.pipeInput(customerId, value)

        val savedValue = store.get(customerId)

        val newInstant = Instant.parse("2022-04-20T14:59:00Z")
        val newValue = CustomerEvent(customerId, newInstant)
        inputTopic.pipeInput(customerId, newValue)

        val newSavedValue = store.get(customerId)

        assertThat(savedValue.timestamp).isNotEqualTo(newSavedValue.timestamp)
        assertThat(savedValue.timestamp).isEqualTo(instant)
        assertThat(newSavedValue.timestamp).isEqualTo(newInstant)
    }


    @Test
    fun `test given customer not seen for more than 5 days when receive event trigger active customer event`() {
        val instant = Instant.parse("2022-04-15T11:59:00Z")
        val value = CustomerEvent(customerId, instant)
        inputTopic.pipeInput(customerId, value)

        val newInstant = Instant.parse("2022-04-20T14:59:00Z")
        val newValue = CustomerEvent(customerId, newInstant)
        inputTopic.pipeInput(customerId, newValue)

        assertThat(outputTopic.isEmpty).isFalse

        val expectedValue = outputTopic.readValue()
        assertThat(expectedValue).isNotNull
        assertThat(expectedValue.daysSinceActive).isEqualTo(5)
    }
}