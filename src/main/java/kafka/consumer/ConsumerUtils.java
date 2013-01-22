package kafka.consumer;

import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class ConsumerUtils {

    private static Logger LOG = LoggerFactory.getLogger(ConsumerUtils.class);

    public static ConsumerConfig getConfiguration(Configuration conf) {
        Properties props = new Properties();
        props.put("zk.connect", conf.get("kafka.zk.connect","localhost:2182"));
        props.put("zk.connectiontimeout.ms", conf.get("kafka.zk.connectiontimeout.ms","1000000"));
        
        ConsumerConfig csConfig = new ConsumerConfig(props);
        return csConfig;
    }
    
}
