package example;

import example.model.ObjectToConsume;
import org.springframework.cloud.function.adapter.aws.SpringBootKinesisEventHandler;



//public class Handler implements RequestHandler<KinesisEvent, Void> {
public class Handler extends SpringBootKinesisEventHandler<ObjectToConsume,Void> {


   /* private static final String toAddress = "saad.gonzalo.ale@gmail.com";

    @Override
    public Void handleRequest(KinesisEvent event, Context context) {
        context.getLogger().log("Inside Lambda....\n");
        StringBuilder sb = new StringBuilder("Hello Prone from lambda and kinesis! \n");

        int record = 1;

        for (KinesisEvent.KinesisEventRecord e: event.getRecords()){
            sb.append("Record: ").append(record).append("\n");
            sb.append("Region: ").append(e.getAwsRegion()).append("\n");
            sb.append("Data: ").append(e.getKinesis().getData());
            record++;
        }

        context.getLogger().log(sb.toString());

        return null;
    }*/
}
