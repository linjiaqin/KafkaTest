import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.api.RpcClient;
import org.apache.flume.api.RpcClientFactory;
import org.apache.flume.event.EventBuilder;

import java.nio.charset.Charset;
import java.util.Properties;

class MyRpcClientFacadeFail {
    private RpcClient client;
    private String hostname;
    private int port;

    /*
    这边代码设三个host用于故障转移，这里偷懒，用同一个主机的3个端口模拟。
    代码还是将Hello Flume 发送10遍给第一个flume代理，当第一个代理故障的时候，
    则发送给第二个代理，以顺序进行故障转移
    flume-ng agent -c conf -f $FLUME_HOME/conf/avro1.conf -n a1 -Dflume.root.logger=INFO,console
    flume-ng agent -c conf -f $FLUME_HOME/conf/avro.conf -n a1 -Dflume.root.logger=INFO,console
     */
    public void init() {
        Properties props = new Properties();
        props.put("client.type", "default_failover");

        // List of hosts (space-separated list of user-chosen host aliases)
        props.put("hosts", "h1 h2 h3");

        // host/port pair for each host alias
        String host1 = "127.0.0.1:41414";
        String host2 = "127.0.0.1:50001";
        String host3 = "127.0.0.1:50002";
        props.put("hosts.h1", host1);
        props.put("hosts.h2", host2);
        props.put("hosts.h3", host3);

        // create the client with failover properties
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
        }
    }

    public void cleanUp() {
        // Close the RPC connection
        client.close();
    }
}
/*
我们把第一个终端的进程关掉，再运行一遍client程序，然后会发现这个时候是发生到第二个终端中。
当第二个终端也关闭的时候，再发送数据，则是发送到最后一个终端。
这里我们可以看到，故障转移的代理主机转移是采用顺序序列的。
 */
public class FlumeFailoverClient {
    public static void main(String[] args) {
        MyRpcClientFacadeFail client = new MyRpcClientFacadeFail();
        // Initialize client with the remote Flume agent's host and port
        client.init();

        // Send 10 events to the remote Flume agent. That agent should be
        // configured to listen with an AvroSource.
        String sampleData = "Hello Flume!";
        for (int i = 0; i < 10; i++) {
            client.sendDataToFlume(sampleData);
        }

        client.cleanUp();
    }
}
