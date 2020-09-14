package main.java.org.arkcase.kafka.performance.service;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.springframework.stereotype.Service;
import tech.allegro.schema.json2avro.converter.JsonAvroConverter;

/**
 * Created by Vladimir Cherepnalkovski <vladimir.cherepnalkovski@armedia.com> on Feb, 2020
 */
@Service
public class JsonAvroMapperService
{
    private final JsonAvroConverter converter;

    public JsonAvroMapperService()
    {
        this.converter = new JsonAvroConverter();
    }

    /** GenericRecord to json converter
     *
     * @param record - Avro serialized GenericRecord
     * @return - JSON String
     */
    public String convertToJson(GenericRecord record)
    {
        if(record != null)
        {
            byte[] binaryJson = converter.convertToJson(record);
            return new String(binaryJson);
        }
        return "Unable to convert empty record.";
    }

    /** Json to GenericRecord converter
     *
     * @param jsonMessage  - json to be converted
     * @param avroSchema - Avro Schema used to serialize jsonMessage.
     * @return Avro serialized GenericRecord.
     */
    public GenericData.Record convertToAvro(String jsonMessage, Schema avroSchema)
    {
        // conversion to GenericData.Record
        return converter.convertToGenericDataRecord(jsonMessage.getBytes(), avroSchema);
    }
}

