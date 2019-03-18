import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.api.RpcClient;
import org.apache.flume.api.RpcClientFactory;
import org.apache.flume.event.EventBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.Charset;
import java.util.Properties;

/*
可以通过flume-ng的配置来实现，不一定通过代码
 */
class MyLBClientFacade{
    private static final Logger logger = LoggerFactory.getLogger(MyLBClientFacade.class);
    private RpcClient client;
    private String hostname;
    private int port;
    public void init(){
        // Setup properties for the load balancing
        Properties props = new Properties();
        props.put("client.type", "default_loadbalance");

// List of hosts (space-separated list of user-chosen host aliases)
        props.put("hosts", "h1 h2 h3");

// host/port pair for each host alias
        String host1 = "127.0.0.1:41414";
        String host2 = "127.0.0.1:50001";
        String host3 = "127.0.0.1:50002";
        props.put("hosts.h1", host1);
        props.put("hosts.h2", host2);
        props.put("hosts.h3", host3);

        props.put("host-selector", "random"); // For random host selection
// props.put("host-selector", "round_robin"); // For round-robin host
//                                            // selection
        props.put("backoff", "true"); // Disabled by default.

        props.put("maxBackoff", "10000"); // Defaults 0, which effectively
        // becomes 30000 ms

// Create the client with load balancing properties
        client = RpcClientFactory.getInstance(props);
    }

    public void sendDataToFlume(String data) {
        // Create a Flume Event object that encapsulates the sample data
        Event event = EventBuilder.withBody(data, Charset.forName("UTF-8"));

        // Send the event
        try {
            client.append(event);
        } catch (EventDeliveryException e) {
            // clean up and recreate the client
            client.close();
            client = null;
            client = RpcClientFactory.getDefaultInstance(hostname, port);
            // Use the following method to create a thrift client (instead of the above line):
            // this.client = RpcClientFactory.getThriftInstance(hostname, port);
        }
    }

    public void cleanUp() {
        // Close the RPC connection
        client.close();
    }
}
/*
还是需要将三个flume source开启
 */
public class FlumeLoadBalancClient {
    public static void main(String[] args) {
        MyLBClientFacade client = new MyLBClientFacade();
        client.init();
        String sampleData = "Hello Flume!";
        for (int i = 0; i < 10; i++) {
            client.sendDataToFlume(sampleData);
        }

        client.cleanUp();
    }
}
