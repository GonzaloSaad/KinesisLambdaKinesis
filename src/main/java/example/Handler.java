package example;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.events.KinesisEvent;
import example.modelcrew.FlightVO;
import org.springframework.cloud.function.adapter.aws.SpringBootKinesisEventHandler;

import java.util.ArrayList;
import java.util.List;


public class Handler extends SpringBootKinesisEventHandler<FlightVO,Void> {

    @Override
    public List<Void> handleRequest(KinesisEvent event, Context context) {
        System.out.println(System.getenv().get("kinesis"));
        List<Void> results = new ArrayList<>();
        context.getLogger().log(event.toString());

        try {
            results = super.handleRequest(event, context);
            context.getLogger().log("Success.\n");
        } catch (Exception e) {
            context.getLogger().log("Error: " + e.getMessage() + "\n");
            context.getLogger().log("Cause: " + e.getCause() + "\n");
            e.printStackTrace();
        }

        return results;
    }


}
