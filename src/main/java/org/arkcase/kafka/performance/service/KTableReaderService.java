package main.java.org.arkcase.kafka.performance.service;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.lang3.time.StopWatch;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.arkcase.kafka.performance.configuration.KTableConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

/**
 * Created by Vladimir Cherepnalkovski <vladimir.cherepnalkovski@armedia.com> on Mar, 2020
 */
@Component
public class KTableReaderService
{
    private final KafkaStreams kafkaStreams;
    private final ProducerService producerService;
    private final Logger logger = LoggerFactory.getLogger(this.getClass());

    public KTableReaderService(KafkaStreams kafkaStreams, ProducerService producerService) {
        this.kafkaStreams = kafkaStreams;
        this.producerService = producerService;
    }

    public String getUser(String userId)
    {
        StopWatch stopWatch = new StopWatch();
        stopWatch.start();
        final ReadOnlyKeyValueStore<String, GenericRecord> result = kafkaStreams.store(KTableConfiguration.generatedMessagesStore,
                QueryableStoreTypes.keyValueStore());
        GenericRecord record = result.get(userId);
        stopWatch.stop();
        if(record != null)
            {
                logger.info("Execution time : {}, UserId : {}, Name : {}", stopWatch.getTime(), record.get("userId"), record.get("name"));
                JsonObject jsonObject = new JsonObject();
                jsonObject.add("result", JsonParser.parseString(record.toString()));
                jsonObject.addProperty("timeInMilliseconds", stopWatch.getTime());
                return jsonObject.toString();
            }
            return null;
    }

    public void deleteUser(String userId)
    {
        producerService.deleteUser(userId);
    }
}
