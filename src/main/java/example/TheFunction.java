package example;

import com.amazonaws.services.kinesis.AmazonKinesis;
import com.amazonaws.services.kinesis.AmazonKinesisClientBuilder;
import com.amazonaws.services.kinesis.model.CreateStreamRequest;
import com.amazonaws.services.kinesis.model.PutRecordRequest;
import com.amazonaws.services.kinesis.model.PutRecordResult;
import example.model.ObjectToConsume;
import example.model.ObjectToProduce;
import example.util.MailSender;
import org.springframework.stereotype.Component;

import java.nio.ByteBuffer;
import java.util.function.Consumer;
import com.amazonaws.services.kinesis.AmazonKinesisClient;

@Component("kinesisFunction")
public class TheFunction implements Consumer<ObjectToConsume>{

    private static final String streamName = "KinesisFromLambdaX242698";
    private static final String partitionKey = "partition-1";

    @Override
    public void accept(ObjectToConsume objectToConsume) {

        System.out.println(objectToConsume);
        ObjectToProduce object = objectToConsume.toProduce();
        System.out.println(object.toString());
        PutRecordRequest request = new PutRecordRequest()
                .withStreamName(streamName)
                .withPartitionKey(partitionKey)
                .withData(ByteBuffer.wrap(object.toString().getBytes()));

        AmazonKinesis client = AmazonKinesisClientBuilder.defaultClient();
        PutRecordResult result = client.putRecord(request);
        System.out.println("Wrote to kinesis...");
        System.out.println("Seq number: "+ result.getSequenceNumber());
        System.out.println("Shard id: "+ result.getShardId());
    }
}
