package main.java.org.arkcase.kafka.performance.service;

import com.google.gson.JsonObject;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestClientException;
import org.springframework.web.client.RestTemplate;

import java.util.Random;

/**
 * Created by Vladimir Cherepnalkovski <vladimir.cherepnalkovski@armedia.com> on Mar, 2020
 */
@Component
public class RandomMessagesGenerator implements Runnable
{
    private final SchemaService schemaService;
    private final JsonAvroMapperService jsonAvroMapperService;
    private final ProducerService producerService;
    private final Logger logger = LoggerFactory.getLogger(this.getClass());
    private final RestTemplate restTemplate;

    private int leftLimit = 97; // letter 'a'
    private int rightLimit = 122; // letter 'z'
    private int targetStringLength = 6;

    public RandomMessagesGenerator(SchemaService schemaService, JsonAvroMapperService jsonAvroMapperService, ProducerService producerService, RestTemplate restTemplate, RestTemplate restTemplate1) {
        this.schemaService = schemaService;
        this.jsonAvroMapperService = jsonAvroMapperService;
        this.producerService = producerService;
        this.restTemplate = restTemplate1;
    }

    public String getRequest(String userId)
    {
        String uri = String.format("http://localhost:9098/user/%s", userId);

        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);

        HttpEntity<String> request = new HttpEntity<>(headers);
        try {
            ResponseEntity<String> result = restTemplate.getForEntity(uri, String.class, request);
            return result.getBody();
        } catch (RestClientException e) {
            e.printStackTrace();
        }
        return "Not found";
    }

    private void generateMessages() throws Exception
    {
        Random random = new Random();
        Schema uiAvroSchema = schemaService.getSchemaFromSchemaServer("generated_messages", null);
        logger.info("Start Generating Messages");
        for (int i = 0; i <= 60000; i++)
        {
            int number = random.nextInt(100000);
            JsonObject jsonObject = new JsonObject();
            jsonObject = createJsonObject("user" + number , number, getName(), getLastName());
            if(i == 10000 || i == 20000 || i == 30000 || i == 40000 || i == 50000 || i ==60000)
            {
                if(i == 10000)
                {
                    logger.info("***************************** 10000 USER UPDATED  *****************************************");
                    jsonObject = createJsonObject("user1122" , number, "Vladimir", "10000");
                }
                if(i == 20000)
                {
                    logger.info("***************************** 20000 USER UPDATED  *****************************************");
                    jsonObject = createJsonObject("user1122" , number, "Vladimir", "20000");
                }
                if(i == 30000)
                {
                    logger.info("***************************** 30000 USER UPDATED  *****************************************");
                    jsonObject = createJsonObject("user1122" , number, "Vladimir", "30000");
                }
                if(i == 40000)
                {
                    logger.info("***************************** 40000 USER UPDATED  *****************************************");
                    jsonObject = createJsonObject("user1122" , number, "Vladimir", "40000");
                }
                if(i == 50000)
                {
                    logger.info("***************************** 50000 USER UPDATED  *****************************************");
                    jsonObject = createJsonObject("user1122" , number, "Vladimir", "50000");
                }
                if(i == 60000)
                {
                    logger.info("***************************** 60000 USER UPDATED  *****************************************");
                    jsonObject = createJsonObject("user1122" , number, "Vladimir", "60000");
                }

                GenericData.Record record = jsonAvroMapperService.convertToAvro(jsonObject.toString(), uiAvroSchema);
                producerService.sendGeneratedMessage(record);

                // Sleep one seccond to check if this delay if long enough for KTable to index all messages.
                Thread.sleep(1000);
                String result = getRequest("user1122");
                logger.info("Result ={}", result);
            }
            else
            {
                GenericData.Record record = jsonAvroMapperService.convertToAvro(jsonObject.toString(), uiAvroSchema);
                producerService.sendGeneratedMessage(record);
            }
        }
        logger.info("End Generating Messages");
    }

    private JsonObject createJsonObject(String userId, int number, String name, String lastName) {
        JsonObject jsonObject = new JsonObject();
        jsonObject.addProperty("userId", userId);
        jsonObject.addProperty("tenantId", getTenant(number));
        jsonObject.addProperty("name", name);
        jsonObject.addProperty("lastName", lastName);
        return jsonObject;
    }

    private String getTenant(Integer number)
    {
        int testNumber = number;
        if (testNumber % 4 == 0)
        {
            return "ArkCase";
        }
        else if (testNumber % 3 == 0)
        {
            return "HDS";
        }
        else if (testNumber % 2 == 0)
        {
            return "BCGU";
        }
        else
        {
            return "DCOCFO";
        }
    }

    private String getName()
    {
        Random random = new Random();

        return random.ints(leftLimit, rightLimit + 1)
                .limit(targetStringLength)
                .collect(StringBuilder::new, StringBuilder::appendCodePoint, StringBuilder::append)
                .toString();
    }

    private String getLastName()
    {
        Random random = new Random();

        return random.ints(leftLimit, rightLimit + 1)
                .limit(targetStringLength)
                .collect(StringBuilder::new, StringBuilder::appendCodePoint, StringBuilder::append)
                .toString();
    }

    @Override
    public void run() {
        try {
            generateMessages();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    //@Scheduled(fixedDelay = 1000000000, initialDelay = 1000)
    public void startMultiThreading()
    {
        Thread t1 = new Thread(this, "thread1");
        t1.start();
        logger.info("Thread 1 has started");
        /*Thread t2 = new Thread(this, "thread2");
        t2.start();*/
        //logger.info("Thread 2 has started");
    }
}
