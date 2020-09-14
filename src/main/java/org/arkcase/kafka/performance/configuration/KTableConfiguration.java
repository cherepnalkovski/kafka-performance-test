package main.java.org.arkcase.kafka.performance.configuration;

import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import io.confluent.kafka.streams.serdes.avro.GenericAvroSerde;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.state.KeyValueStore;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.Map;
import java.util.Properties;

import static java.util.Collections.singletonMap;

/**
 * Created by Vladimir Cherepnalkovski <vladimir.cherepnalkovski@armedia.com> on Mar, 2020
 */
@Configuration
public class KTableConfiguration
{
    public static final String generatedMessagesStore = "generated_messages_list";

    @Value(value = "${spring.kafka.bootstrapAddress}")
    private String bootstrapAddress;
    @Value(value = "${spring.kafka.schemaRegistryAddress}")
    private String schemaRegistryAddress;
    @Value(value = "${server.port}")
    private Integer serverPort;
    @Value(value = "${spring.kafka.streams.state-dir}")
    private String stateDir;
    @Value(value = "${spring.kafka.streams.host}")
    private String host;

    private static Topology buildTopology(final Map<String, String> serdeConfig)
    {
        final GenericAvroSerde valueSerde = new GenericAvroSerde();
        valueSerde.configure(serdeConfig, false);

        final StreamsBuilder builder = new StreamsBuilder();

        // get table and create a state store to hold all generated messages items in the store.
        // We are using this store to test performance with million of messages.
        final KTable<String, GenericRecord>
                generatedMessages =
                builder.table(KafkaTopicConfiguration.generatedMessagesTopic, Materialized.<String, GenericRecord, KeyValueStore<Bytes, byte[]>>as(generatedMessagesStore)
                        .withKeySerde(Serdes.String())
                        .withValueSerde(valueSerde));

        return builder.build();
    }

    private static Properties streamsConfig(final String bootstrapServers,
                                            final int applicationServerPort,
                                            final String stateDir,
                                            final String host)
    {
        final Properties streamsConfiguration = new Properties();
        streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, "kafka-ksream-performace-test");
        streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        streamsConfiguration.put(StreamsConfig.APPLICATION_SERVER_CONFIG, host + ":" + applicationServerPort);
        streamsConfiguration.put(StreamsConfig.STATE_DIR_CONFIG, stateDir);
        streamsConfiguration.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        streamsConfiguration.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 200);
        streamsConfiguration.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 10);

        return streamsConfiguration;
    }

    @Bean
    public KafkaStreams kafkaStreams()
    {
        KafkaStreams streams = new KafkaStreams(
                buildTopology(singletonMap(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryAddress)),
                streamsConfig(bootstrapAddress, serverPort, stateDir, host)
        );
        streams.start();
        return streams;
    }
}
