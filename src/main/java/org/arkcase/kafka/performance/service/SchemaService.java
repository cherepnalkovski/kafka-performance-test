package main.java.org.arkcase.kafka.performance.service;

import org.apache.avro.Schema;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Service;
import org.springframework.util.StringUtils;
import org.springframework.web.client.RestTemplate;

/**
 * Created by Vladimir Cherepnalkovski <vladimir.cherepnalkovski@armedia.com> on Feb, 2020
 * This is copy from SchemaService. I am using copy class that so we don't have to create communication between those microservices only for this POC.
 */
@Service
public class SchemaService
{
    @Value(value = "${spring.kafka.schemaRegistryAddress}")
    private String schemaRegistryAddress;

    private final RestTemplate restTemplate;

    public SchemaService(RestTemplate restTemplate)
    {
        this.restTemplate = restTemplate;
    }

    Schema getSchemaFromSchemaServer(String schemaName, String versionId) throws Exception
    {
        String schemaByNameAndVersion = getSchemaByNameAndVersion(schemaName, versionId);

        return new Schema.Parser().parse(schemaByNameAndVersion);
    }

    private String getSchemaByNameAndVersion(String schemaName, String versionId) throws Exception
    {
        versionId = StringUtils.isEmpty(versionId) ? "latest" : versionId;
        String uri = String.format("%s/subjects/%s-value/versions/%s/schema", schemaRegistryAddress, schemaName, versionId);

        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.valueOf("application/vnd.schemaregistry.v1+json"));

        HttpEntity<String> request = new HttpEntity<>(headers);
        try
        {
            ResponseEntity<String> result = restTemplate.getForEntity(uri, String.class, request);
            return result.getBody();
        }
        catch (Exception e)
        {
            throw new Exception("Schema not found", e);
        }
    }
}
