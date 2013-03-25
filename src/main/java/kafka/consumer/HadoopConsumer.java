package kafka.consumer;

import org.apache.commons.cli.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.FileInputStream;
import java.io.IOException;

public class HadoopConsumer extends Configured implements Tool {

    static {
        Configuration.addDefaultResource("core-site.xml");
        //Configuration.addDefaultResource("mapred-site.xml");
    }

    private CommandLine cmd;


    
    public static class KafkaMapper extends Mapper<LongWritable, BytesWritable, LongWritable, Text> {
        @Override
        public void map(LongWritable key, BytesWritable value, Context context) throws IOException {
            Text out = new Text();
            try {
                out.set(value.getBytes(),0, value.getLength());
                context.write(key, out);
            } catch (InterruptedException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }
        
    }


    @Override
    public int run(String[] args) throws Exception {
        
        //ToolRunner.printGenericCommandUsage(System.err);
        /*
        if (args.length < 2) {
            ToolRunner.printGenericCommandUsage(System.err);
            return -1;
        }
        */

        cmd = getOptions(args);
        //HelpFormatter formatter = new HelpFormatter();
        //formatter.printHelp("kafka.consumer.hadoop", options);

        Configuration conf = getConf();

        String confPath = cmd.getOptionValue("job-conf");
        if (confPath != null)
            conf.addResource(new FileInputStream(confPath), confPath);

        // TODO: Allow conf file to set these properties
        String kafkaTopic = cmd.getOptionValue("topic", "test");
        String kafkaGroup = cmd.getOptionValue("consumer-group", "test_group");
        conf.set("kafka.topic", kafkaTopic);
        conf.set("kafka.groupid", kafkaGroup);
        conf.set("kafka.zk.connect", cmd.getOptionValue("zk-connect", "localhost:2182"));
        if (cmd.getOptionValue("autooffset-reset") != null)
            conf.set("kafka.autooffset.reset", cmd.getOptionValue("autooffset-reset"));
        conf.setInt("kafka.limit", Integer.valueOf(cmd.getOptionValue("limit", "-1")));

        conf.setBoolean("mapred.map.tasks.speculative.execution", false);
        String jobQueue = cmd.getOptionValue("queue");
        if (jobQueue != null) {
            conf.set("mapred.job.queue.name",   jobQueue);
            conf.set("mapreduce.job.queuename", jobQueue);
        }

        String jobName = cmd.getOptionValue("name", "Kafka.Consumer(topic="+kafkaTopic+", group="+kafkaGroup+")");
        Job job = Job.getInstance(conf, jobName);
        job.setJarByClass(getClass());
        job.setMapperClass(KafkaMapper.class);
        // input
        job.setInputFormatClass(KafkaInputFormat.class);
        // output
        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(Text.class);
        job.setOutputFormatClass(KafkaOutputFormat.class);
        
        job.setNumReduceTasks(0);
        
        KafkaOutputFormat.setOutputPath(job, new Path(cmd.getArgs()[0]));
        
        boolean success = job.waitForCompletion(true);
        if (success) {
            commit(conf);
        }
        return success ? 0: -1;
    }

    private void commit(Configuration conf) throws IOException {
        ZkUtils zk = new ZkUtils(conf);
        try {
            String kafkaTopic = conf.get("kafka.topic");
            String kafkaGroup = conf.get("kafka.groupid");
            zk.commit(kafkaGroup, kafkaTopic);
        } catch (Exception e) {
            rollback();
        } finally {
            zk.close();
        }
    }

    private void rollback() {
    }

    private CommandLine getOptions(String[] args) throws ParseException {
        if (cmd != null) return cmd;

        CommandLineParser parser = new PosixParser();
        cmd = parser.parse(buildCLIOptions(), args);
        return cmd;
    }

    @SuppressWarnings("static-access")
    private static Options buildCLIOptions() {
        Options options = new Options();
        
        options.addOption(
            OptionBuilder.withArgName("topic")
                .withLongOpt("topic")
                .hasArg()
                .withDescription("Kafka topic")
                .create("t"));
        options.addOption(
            OptionBuilder.withArgName("groupid")
                .withLongOpt("consumer-group")
                .hasArg()
                .withDescription("Kafka consumer groupid")
                .create("g"));
        options.addOption(
            OptionBuilder.withArgName("zk")
                .withLongOpt("zk-connect")
                .hasArg()
                .withDescription("ZooKeeper connection string")
                .create("z"));
        options.addOption(
            OptionBuilder.withArgName("offset")
                .withLongOpt("autooffset-reset")
                .hasArg()
                .withDescription("Kafka offset reset")
                .create("o"));
        options.addOption(
            OptionBuilder.withArgName("limit")
                .withLongOpt("limit")
                .hasArg()
                .withDescription("Kafka limit")
                .create("l"));
        options.addOption(
            OptionBuilder.withArgName("name")
                .withLongOpt("name")
                .hasArg()
                .withDescription("Hadoop job name")
                .create("n"));
        options.addOption(
            OptionBuilder.withArgName("queue")
                .withLongOpt("queue")
                .hasArg()
                .withDescription("Hadoop job queue name")
                .create("q"));
        options.addOption(
            OptionBuilder.withArgName("conf")
                .withLongOpt("job-conf")
                .hasArg()
                .withDescription("Hadoop job configuration file")
                .create("C"));

        return options;
    }

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new HadoopConsumer(), args);
        System.exit(exitCode);
    }

}
