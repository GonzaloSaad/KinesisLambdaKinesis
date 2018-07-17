package example;

import com.amazonaws.services.kinesis.AmazonKinesis;
import com.amazonaws.services.kinesis.AmazonKinesisClientBuilder;
import com.amazonaws.services.kinesis.model.PutRecordRequest;
import com.amazonaws.services.kinesis.model.PutRecordResult;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import example.model.ObjectToConsume;
import example.model.ObjectToProduce;
import example.modelcrew.FlightVO;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.function.Consumer;

@Component("kinesisFunction")
public class TheFunction implements Consumer<FlightVO>{

    private static final String streamName = "KinesisFromLambdaX242698";
    private static final String partitionKey = "partition-1";

    @Override
    public void accept(FlightVO flightVO) {

        flightVO.setSuffix("A");
        String json = getJsonFormat(flightVO);
        System.out.println("Json: "+ json);

        PutRecordRequest request = new PutRecordRequest()
                .withStreamName(streamName)
                .withPartitionKey(partitionKey)
                .withData(ByteBuffer.wrap(json.getBytes()));

        AmazonKinesis client = AmazonKinesisClientBuilder.defaultClient();
        PutRecordResult result = client.putRecord(request);
        System.out.println("Wrote to kinesis...");
        System.out.println("Seq number: "+ result.getSequenceNumber());
        System.out.println("Shard id: "+ result.getShardId());
        client.shutdown();
    }

    public String getJsonFormat(Object object) {
        String json = "EMPTY";
        ObjectMapper mapper = new ObjectMapper();
        mapper.configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false);
        mapper.configure(SerializationFeature.WRITE_DATE_TIMESTAMPS_AS_NANOSECONDS, false);
        JavaTimeModule module = new JavaTimeModule();
        mapper.registerModule(module);
        try {
            json = mapper.writeValueAsString(object);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return json;
    }
}
