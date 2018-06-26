
package local_project.sparkstreaming_local_basics_j06_230_sparkjob1_0_1;

import org.apache.commons.codec.binary.Base64;
import org.apache.commons.codec.binary.StringUtils;
import org.apache.commons.lang.SerializationUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.kafka010.CanCommitOffsets;
import org.apache.spark.streaming.kafka010.HasOffsetRanges;
import org.apache.spark.streaming.kafka010.OffsetRange;
import routines.TalendString;
import routines.system.BigDataParserUtils;
import routines.system.SparkStreamingRunStat;
import routines.system.SparkStreamingRunStat.StatBean;
import routines.system.api.TalendJob;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;


@SuppressWarnings("unused")

/**
 * Job: sparkstreaming_local_basics_j06_230_sparkjob1 Purpose: <br>
 * Description:  <br>
 * Spark: SPARK_2_2 <br>
 * @author toto@toto.co
 * @version 6.2.0
 * @status 
 */
public class sparkstreaming_local_basics_j06_230_sparkjob1 implements TalendJob {
    static {
        System.setProperty("TalendJob.log", "sparkstreaming_local_basics_j06_230_sparkjob1.log");
    }
    private final static String utf8Charset = "UTF-8";
    private GlobalVar globalMap = null;


        private static org.apache.log4j.Logger LOG = org.apache.log4j.Logger
            .getLogger(sparkstreaming_local_basics_j06_230_sparkjob1.class);
        // DI compatibility
        private static org.apache.log4j.Logger log = LOG;

    private static class GlobalVar {
        public static final String GLOBALVAR_PARAMS_PREFIX = "talend.globalvar.params.";
        private Configuration job;
        private java.util.Map<String, Object> map;

        public GlobalVar(Configuration job) {
            this.job = job;
            this.map = new java.util.HashMap<String, Object>();
        }

        public Object get(String key) {
            String tempValue = job.get(GLOBALVAR_PARAMS_PREFIX + key);
            if (tempValue != null) {
                return SerializationUtils.deserialize(Base64
                        .decodeBase64(StringUtils.getBytesUtf8(tempValue)));
            } else {
                return null;
            }
        }

        public void put(String key, Object value) {
            if (value == null)
                return;
            job.set(GLOBALVAR_PARAMS_PREFIX + key, StringUtils
                    .newStringUtf8(Base64.encodeBase64(SerializationUtils
                            .serialize((Serializable) value))));
        }

        public void putLocal(String key, Object value) {
            map.put(key, value);
        }

        public Object getLocal(String key) {
            return map.get(key);
        }
    }

    // create and load default properties
    private java.util.Properties defaultProps = new java.util.Properties();

    public static class ContextProperties extends java.util.Properties {

        private static final long serialVersionUID = 1L;

        public static final String CONTEXT_FILE_NAME = "talend.context.fileName";
        public static final String CONTEXT_KEYS = "talend.context.keys";
        public static final String CONTEXT_PARAMS_PREFIX = "talend.context.params.";
        public static final String CONTEXT_PARENT_KEYS = "talend.context.parent.keys";
        public static final String CONTEXT_PARENT_PARAMS_PREFIX = "talend.context.parent.params.";

        public ContextProperties(java.util.Properties properties){
            super(properties);
        }

        public ContextProperties(){
            super();
        }

        public ContextProperties(Configuration job){
            super();
            String contextFileName = (String) job.get(CONTEXT_FILE_NAME);
            try {
                if(contextFileName != null && !"".equals(contextFileName)){
                    java.io.File contextFile = new java.io.File(contextFileName);
                    if (contextFile.exists()) {
                        java.io.InputStream contextIn = contextFile.toURI().toURL().openStream();
                        this.load(contextIn);
                        contextIn.close();
                    } else {
                        java.io.InputStream contextIn = sparkstreaming_local_basics_j06_230_sparkjob1.class.getClassLoader().getResourceAsStream("local_project/sparkstreaming_local_basics_j06_230_sparkjob1_0_1/contexts/"+contextFileName);
                        if(contextIn != null){
                            this.load(contextIn);
                            contextIn.close();
                        }
                    }
                }
                Object contextKeys = job.get(CONTEXT_KEYS);
                if (contextKeys != null) {
                    java.util.StringTokenizer st = new java.util.StringTokenizer(contextKeys.toString(), " ");
                    while (st.hasMoreTokens()) {
                        String contextKey = st.nextToken();
                        if((String) job.get(CONTEXT_PARAMS_PREFIX + contextKey) != null){
                            this.put(contextKey, job.get(CONTEXT_PARAMS_PREFIX + contextKey));
                        }
                    }
                }
                Object contextParentKeys = job.get(CONTEXT_PARENT_KEYS);
                if (contextParentKeys != null) {
                    java.util.StringTokenizer st = new java.util.StringTokenizer(contextParentKeys.toString(), " ");
                    while (st.hasMoreTokens()) {
                        String contextKey = st.nextToken();
                        if((String)job.get(CONTEXT_PARENT_PARAMS_PREFIX + contextKey) != null){
                            this.put(contextKey, job.get(CONTEXT_PARENT_PARAMS_PREFIX + contextKey));
                        }
                    }
                }

                this.loadValue(null,job);
            } catch (java.io.IOException ie) {
                System.err.println("Could not load context " + contextFileName);
                ie.printStackTrace();
            }
        }

        public void synchronizeContext(){
                if(kafka_broker_host != null){
                        this.setProperty("kafka_broker_host", kafka_broker_host.toString());
                }
                if(kafka_broker_port != null){
                        this.setProperty("kafka_broker_port", kafka_broker_port.toString());
                }
                if(kafka_topic != null){
                        this.setProperty("kafka_topic", kafka_topic.toString());
                }
                if(result_table != null){
                        this.setProperty("result_table", result_table.toString());
                }
                if(data_output_dir != null){
                        this.setProperty("data_output_dir", data_output_dir.toString());
                }
                if(result_port != null){
                        this.setProperty("result_port", result_port.toString());
                }
                if(result_username != null){
                        this.setProperty("result_username", result_username.toString());
                }
                if(local_tmp_dir != null){
                        this.setProperty("local_tmp_dir", local_tmp_dir.toString());
                }
                if(param_file_path != null){
                        this.setProperty("param_file_path", param_file_path.toString());
                }
                if(result_host != null){
                        this.setProperty("result_host", result_host.toString());
                }
                if(result_database != null){
                        this.setProperty("result_database", result_database.toString());
                }
                if(result_password != null){
                        this.setProperty("result_password", result_password.toString());
                }
                if(data_dir != null){
                        this.setProperty("data_dir", data_dir.toString());
                }
        }

                public String kafka_broker_host;
                public String getKafka_broker_host(){
                    return this.kafka_broker_host;
                }
                public String kafka_broker_port;
                public String getKafka_broker_port(){
                    return this.kafka_broker_port;
                }
                public String kafka_topic;
                public String getKafka_topic(){
                    return this.kafka_topic;
                }
                public String result_table;
                public String getResult_table(){
                    return this.result_table;
                }
                public String data_output_dir;
                public String getData_output_dir(){
                    return this.data_output_dir;
                }
                public String result_port;
                public String getResult_port(){
                    return this.result_port;
                }
                public String result_username;
                public String getResult_username(){
                    return this.result_username;
                }
                public String local_tmp_dir;
                public String getLocal_tmp_dir(){
                    return this.local_tmp_dir;
                }
                public String param_file_path;
                public String getParam_file_path(){
                    return this.param_file_path;
                }
                public String result_host;
                public String getResult_host(){
                    return this.result_host;
                }
                public String result_database;
                public String getResult_database(){
                    return this.result_database;
                }
                public String result_password;
                public String getResult_password(){
                    return this.result_password;
                }
                public String data_dir;
                public String getData_dir(){
                    return this.data_dir;
                }
        public void loadValue(java.util.Properties context_param, Configuration job){
                    this.kafka_broker_host=(String) this.getProperty("kafka_broker_host");
                    this.kafka_broker_port=(String) this.getProperty("kafka_broker_port");
                    this.kafka_topic=(String) this.getProperty("kafka_topic");
                    this.result_table=(String) this.getProperty("result_table");
                    this.data_output_dir=(String) this.getProperty("data_output_dir");
                    this.result_port=(String) this.getProperty("result_port");
                    this.result_username=(String) this.getProperty("result_username");
                    this.local_tmp_dir=(String) this.getProperty("local_tmp_dir");
                    this.param_file_path=(String) this.getProperty("param_file_path");
                    this.result_host=(String) this.getProperty("result_host");
                    this.result_database=(String) this.getProperty("result_database");
                    this.result_password=(String) this.getProperty("result_password");
                    this.data_dir=(String) this.getProperty("data_dir");
        }
    }
    private ContextProperties context = new ContextProperties();
    public ContextProperties getContext() {
        return this.context;
    }

        public static class TalendSparkStreamingListener extends
                org.apache.spark.streaming.ui.StreamingJobProgressListener {

            private int batchCompleted = 0;
            private int batchStarted = 0;
            private String lastProcessingDelay = "";
            private String lastSchedulingDelay = "";
            private String lastTotalDelay = "";


            public TalendSparkStreamingListener(org.apache.spark.streaming.StreamingContext ssc) {
                super(ssc);
                StatBean sb = runStat.createSparkStreamingStatBean();
                sb.setSubjobId("1");
                sb.setBatchCompleted(this.batchCompleted);
                sb.setBatchStarted(this.batchStarted);
                sb.setLastProcessingDelay(this.lastProcessingDelay);
                sb.setLastSchedulingDelay(this.lastSchedulingDelay);
                sb.setLastTotalDelay(this.lastTotalDelay);
                runStat.updateSparkStreamingData(sb);
            }

            @Override
            public void onBatchStarted(org.apache.spark.streaming.scheduler.StreamingListenerBatchStarted batchStarted) {
                super.onBatchStarted(batchStarted);
                this.batchStarted = this.batchStarted + 1;
                StatBean sb = runStat.createSparkStreamingStatBean();
                sb.setSubjobId("1");
                sb.setBatchCompleted(this.batchCompleted);
                sb.setBatchStarted(this.batchStarted);
                sb.setLastProcessingDelay(this.lastProcessingDelay);
                sb.setLastSchedulingDelay(this.lastSchedulingDelay);
                sb.setLastTotalDelay(this.lastTotalDelay);
                runStat.updateSparkStreamingData(sb);
            }

            @Override
            public void onBatchCompleted(org.apache.spark.streaming.scheduler.StreamingListenerBatchCompleted batchCompleted) {
                super.onBatchCompleted(batchCompleted);
                        scala.Option<org.apache.spark.streaming.ui.BatchUIData> lastCompletedBatch = this.lastCompletedBatch();
                synchronized(lastCompletedBatch) {
                    this.batchCompleted = this.batchCompleted + 1;
                    if (!lastCompletedBatch.isEmpty()) this.lastProcessingDelay = lastCompletedBatch.get().processingDelay().get() + "";
                    if (!lastCompletedBatch.isEmpty()) this.lastSchedulingDelay = lastCompletedBatch.get().schedulingDelay().get() + "";
                    if (!lastCompletedBatch.isEmpty()) this.lastTotalDelay = lastCompletedBatch.get().totalDelay().get() + "";
                    StatBean sb = runStat.createSparkStreamingStatBean();
                    sb.setSubjobId("1");
                    sb.setBatchCompleted(this.batchCompleted);
                    sb.setBatchStarted(this.batchStarted);
                    sb.setLastProcessingDelay(this.lastProcessingDelay);
                    sb.setLastSchedulingDelay(this.lastSchedulingDelay);
                    sb.setLastTotalDelay(this.lastTotalDelay);
                    runStat.updateSparkStreamingData(sb);
                }
            }
        }

        private static SparkStreamingRunStat runStat = new SparkStreamingRunStat();

    private final static String jobVersion = "0.1";
    private final static String jobName = "sparkstreaming_local_basics_j06_230_sparkjob1";
    private final static String projectName = "LOCAL_PROJECT";
    public Integer errorCode = null;

    private final java.io.ByteArrayOutputStream baos = new java.io.ByteArrayOutputStream();
    private final java.io.PrintStream errorMessagePS = new java.io.PrintStream(
            new java.io.BufferedOutputStream(baos));

    private static String currentComponent = "";

    public String getExceptionStackTrace() {
        if ("failure".equals(this.getStatus())) {
            errorMessagePS.flush();
            return baos.toString();
        }
        return null;
    }

    //should be remove later
    public void setDataSources(java.util.Map<String, javax.sql.DataSource> dataSources) {

    }

/**
 * [tKafkaInput_1 sparkstreamingcode ] start
 */
	

public static class tKafkaInput_1_ValueDeserializer implements org.apache.kafka.common.serialization.Deserializer<row1Struct> {

	private org.apache.kafka.common.serialization.StringDeserializer stringDeserializer;
	
	public tKafkaInput_1_ValueDeserializer() {
		// empty
	}

	public void configure(java.util.Map<java.lang.String,?> configs, boolean isKey) {
		stringDeserializer = new org.apache.kafka.common.serialization.StringDeserializer();
		stringDeserializer.configure(configs, isKey);
	}

	public row1Struct deserialize(String topic, byte[] data) {
		row1Struct result = new row1Struct();
		    String line = stringDeserializer.deserialize(topic, data);
		    result.payload = line;
		return result;
    }

	public void close() {
		// nothing
	}
}

public static class tKafkaInput_1_KeyDeserializer implements org.apache.kafka.common.serialization.Deserializer<org.apache.hadoop.io.NullWritable> {

	public tKafkaInput_1_KeyDeserializer() {
		// empty
	}
	
	public void configure(java.util.Map<java.lang.String,?> configs, boolean isKey) {
		// nothing
	}

	public org.apache.hadoop.io.NullWritable deserialize(String topic, byte[] data) {
	    return org.apache.hadoop.io.NullWritable.get();
	}

	public void close() {
		// nothing
	}
}

public static class tKafkaInput_1_Function implements org.apache.spark.api.java.function.PairFunction<org.apache.kafka.clients.consumer.ConsumerRecord<NullWritable, row1Struct>, NullWritable, row1Struct> {

	public tKafkaInput_1_Function() {
		// empty
	}
	
	@Override
	public scala.Tuple2<NullWritable, row1Struct> call(org.apache.kafka.clients.consumer.ConsumerRecord<NullWritable, row1Struct> record) {
		return new scala.Tuple2(record.key(), record.value());
	}

}/**
 * [tKafkaInput_1 sparkstreamingcode ] stop
 *//**
 * [tExtractDelimitedFields_1 sparkstreamingcode ] start
 */

            public static class tExtractDelimitedFields_1_Function implements org.apache.spark.api.java.function.PairFlatMapFunction<scala.Tuple2<NullWritable, row1Struct>, NullWritable, row2Struct> {
                private ContextProperties context = new ContextProperties();

                //private ContextProperties context = null;

                public tExtractDelimitedFields_1_Function(JobConf job) {
                    this.context = new ContextProperties(job);
                }

	            public java.util.Iterator<scala.Tuple2<NullWritable, row2Struct>> call(scala.Tuple2<NullWritable, row1Struct> data) throws java.lang.Exception {
	            	java.util.List<scala.Tuple2<NullWritable, row2Struct>> outputs = new java.util.ArrayList<scala.Tuple2<NullWritable, row2Struct>>();row2Struct row2 = new row2Struct(); 
row1Struct row1 = data._2;
	            	
	                
        if (row1.payload == null)
            return null;


        try {
                String[] values = routines.system.StringUtils.splitNotRegex(row1.payload, ";");

            String temp = "";
                if (values.length > 0) {
                        temp = values[0];

                            row2.id = BigDataParserUtils.parseTo_Integer(temp);
                } else {
                        row2.id = null;
                }
                if (values.length > 1) {
                        temp = values[1];
                            row2.name = temp;
                } else {
                        row2.name = null;
                }
                
                
            outputs.add(new scala.Tuple2<NullWritable, row2Struct>(NullWritable.get(), row2));row2 = new row2Struct();
        } catch (RuntimeException ex) {
            // Die immediately on errors.
            throw new IOException(ex);
        }
	                return outputs.iterator();
	            }
	        }
		/**
 * [tExtractDelimitedFields_1 sparkstreamingcode ] stop
 *//**
 * [tLogRow_1 sparkstreamingcode ] start
 */
        
    

        public static class tLogRow_1_ForeachRDDOutput implements org.apache.spark.api.java.function.VoidFunction<org.apache.spark.api.java.JavaPairRDD<NullWritable,row2Struct>> {

            private final ContextProperties context;
            private final String OUTPUT_FIELD_SEPARATOR_tLogRow_1;
            private transient java.io.PrintStream consoleOut_tLogRow_1 = new java.io.PrintStream(new java.io.BufferedOutputStream(System.out));

            public tLogRow_1_ForeachRDDOutput(JobConf job){
                this.context = new ContextProperties(job);
                this.OUTPUT_FIELD_SEPARATOR_tLogRow_1 = "|";
                consoleOut_tLogRow_1 = new java.io.PrintStream(new java.io.BufferedOutputStream(System.out));
                tLogRow_1_createDisplayMode();
            }

            public void tLogRow_1_createDisplayMode(){
            }

            public void call(org.apache.spark.api.java.JavaPairRDD<NullWritable,row2Struct> rdd) throws Exception {

                if(consoleOut_tLogRow_1 == null){
                    consoleOut_tLogRow_1 = new java.io.PrintStream(new java.io.BufferedOutputStream(System.out));
                }

                // Get the values since the keys are not serializable
                java.util.List<row2Struct> recordsList = rdd.values().collect();
                java.util.Iterator<row2Struct> recordsIterator = recordsList.iterator();

                StringBuilder strBuffer_tLogRow_1 = null;
                while (recordsIterator.hasNext()) {
                    row2Struct row2 = recordsIterator.next();
                        strBuffer_tLogRow_1 = new StringBuilder();
                                if(row2.id != null) { //
                            strBuffer_tLogRow_1.append(
                                    String.valueOf(row2.id)
                            );
                                } //
                            strBuffer_tLogRow_1.append("|");
                                if(row2.name != null) { //
                            strBuffer_tLogRow_1.append(
                                    String.valueOf(row2.name)
                            );
                                } //
                        consoleOut_tLogRow_1.println(strBuffer_tLogRow_1.toString());
                        consoleOut_tLogRow_1.flush();
                }
                return;
            }
        }
/**
 * [tLogRow_1 sparkstreamingcode ] stop
 */    public void tKafkaInput_1PostProcess(final org.apache.spark.streaming.api.java.JavaStreamingContext ctx, GlobalVar globalMap) throws java.lang.Exception {
        FileSystem fs = FileSystem.get(ctx.sparkContext().hadoopConfiguration());
        final JobConf job = new JobConf(ctx.sparkContext().hadoopConfiguration());

        try {
	    } catch(java.lang.Exception e) {
	    	throw e;
	    }
    }
        	public void tKafkaInput_1Process(final org.apache.spark.sql.SparkSession ss, final org.apache.spark.streaming.api.java.JavaStreamingContext ctx, GlobalVar globalMap) throws java.lang.Exception {
        	
            FileSystem fs = FileSystem.get(ctx.sparkContext().hadoopConfiguration());
            final JobConf job = new JobConf(ctx.sparkContext().hadoopConfiguration());

            try {
/**
 * [tKafkaInput_1 sparkstreamingconfig ] start
 */
		currentComponent = "tKafkaInput_1";
		

    	class BytesLimit65535_tKafkaInput_1{
    		public void limitLog4jByte() throws Exception{
    			
    		}
    	}
    	
        new BytesLimit65535_tKafkaInput_1().limitLog4jByte();
	



		java.util.Map<String, Object> tKafkaInput_1_kafkaProperties = new java.util.HashMap<String, Object>();
		tKafkaInput_1_kafkaProperties.put("bootstrap.servers", context.kafka_broker_host + ":" + context.kafka_broker_port);
		tKafkaInput_1_kafkaProperties.put(org.apache.kafka.clients.consumer.ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
		tKafkaInput_1_kafkaProperties.put("serializer.encoding", "UTF-8");
		tKafkaInput_1_kafkaProperties.put("group.id", "mygroup");
		tKafkaInput_1_kafkaProperties.put(org.apache.kafka.clients.consumer.ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, tKafkaInput_1_KeyDeserializer.class);
		tKafkaInput_1_kafkaProperties.put(org.apache.kafka.clients.consumer.ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, tKafkaInput_1_ValueDeserializer.class);
		java.util.Set<String> tKafkaInput_1_kafkaTopics = new java.util.HashSet<String>();
				tKafkaInput_1_kafkaTopics.add(context.kafka_topic);

org.apache.spark.streaming.api.java.JavaDStream<org.apache.kafka.clients.consumer.ConsumerRecord<NullWritable, row1Struct>> rdd_kafka_row1 = org.apache.spark.streaming.kafka010.KafkaUtils.createDirectStream(
	ctx, 
	org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent(), 
	org.apache.spark.streaming.kafka010.ConsumerStrategies.<NullWritable, row1Struct>Subscribe(tKafkaInput_1_kafkaTopics, tKafkaInput_1_kafkaProperties)
);


// LAURENT

                String groupId = "mygroup";


                String zkUrl = "zookeeper0.weave.local";
                int zkSessionTimeout = 10000;
                int zkConnectionTimeout = 10000;
                OffsetsStorage offsetsStorage = new OffsetsStorage(zkUrl, zkSessionTimeout, zkConnectionTimeout);

                List<OffsetRange[]> offsetRanges = new ArrayList<OffsetRange[]>();

                // Store processing offsets
                rdd_kafka_row1.foreachRDD(rdd -> {
                    OffsetRange[] ranges = ((HasOffsetRanges) rdd.rdd()).offsetRanges();
                    // Store rdd id offsets pairs
                    offsetsStorage.storeProcessingOffets(groupId, context.kafka_topic, ranges);
                });

                // Standard processing

                // Commit offsets
                rdd_kafka_row1.foreachRDD(rdd -> {
                    // Get processing offsets
                    OffsetRange[] processedOffsets = new OffsetRange[0];
                    // Commit offsets
                    ((CanCommitOffsets) ((JavaInputDStream<ConsumerRecord<NullWritable,row1Struct>>) rdd_kafka_row1).inputDStream()).commitAsync(processedOffsets);
                    // Clean processing offsets
                });



// LAURENT

org.apache.spark.streaming.api.java.JavaPairDStream<NullWritable, row1Struct> rdd_row1 = rdd_kafka_row1.mapToPair(new tKafkaInput_1_Function());


/**
 * [tKafkaInput_1 sparkstreamingconfig ] stop
 *//**
 * [tExtractDelimitedFields_1 sparkstreamingconfig ] start
 */
		currentComponent = "tExtractDelimitedFields_1";
		

    	class BytesLimit65535_tExtractDelimitedFields_1{
    		public void limitLog4jByte() throws Exception{
    			
    		}
    	}
    	
        new BytesLimit65535_tExtractDelimitedFields_1().limitLog4jByte();

            org.apache.spark.streaming.api.java.JavaPairDStream<NullWritable, row2Struct> rdd_row2 = rdd_row1.flatMapToPair(new tExtractDelimitedFields_1_Function(job));/**
 * [tExtractDelimitedFields_1 sparkstreamingconfig ] stop
 *//**
 * [tLogRow_1 sparkstreamingconfig ] start
 */
		currentComponent = "tLogRow_1";
		

    	class BytesLimit65535_tLogRow_1{
    		public void limitLog4jByte() throws Exception{
    			
    		}
    	}
    	
        new BytesLimit65535_tLogRow_1().limitLog4jByte();
    rdd_row2.foreachRDD(new tLogRow_1_ForeachRDDOutput(job));/**
 * [tLogRow_1 sparkstreamingconfig ] stop
 */
	    } catch(java.lang.Exception e) {
	        
	    	throw e;
	    }
    }    public void tSparkConfiguration_1PostProcess(final org.apache.spark.streaming.api.java.JavaStreamingContext ctx, GlobalVar globalMap) throws java.lang.Exception {
        FileSystem fs = FileSystem.get(ctx.sparkContext().hadoopConfiguration());
        final JobConf job = new JobConf(ctx.sparkContext().hadoopConfiguration());

        try {
	    } catch(java.lang.Exception e) {
	    	throw e;
	    }
    }
        	public void tSparkConfiguration_1Process(final org.apache.spark.sql.SparkSession ss, final org.apache.spark.streaming.api.java.JavaStreamingContext ctx, GlobalVar globalMap) throws java.lang.Exception {
        	
            FileSystem fs = FileSystem.get(ctx.sparkContext().hadoopConfiguration());
            final JobConf job = new JobConf(ctx.sparkContext().hadoopConfiguration());

            try {

	    } catch(java.lang.Exception e) {
	        
	    	throw e;
	    }
    }

	
	
	

    
    
    
    
    
    

public static class TalendKryoRegistrator implements org.apache.spark.serializer.KryoRegistrator {
		
	@Override
	public void registerClasses(com.esotericsoftware.kryo.Kryo kryo) {
		try {
			kryo.register(Class.forName("org.talend.bigdata.dataflow.keys.JoinKeyRecord"));
		} catch (java.lang.ClassNotFoundException e) {
			// Ignore
		}
					

		kryo.register(java.net.InetAddress.class, new InetAddressSerializer());
		kryo.addDefaultSerializer(java.net.InetAddress.class, new InetAddressSerializer());
				
		kryo.register(local_project.sparkstreaming_local_basics_j06_230_sparkjob1_0_1.row2Struct.class);
				
		kryo.register(local_project.sparkstreaming_local_basics_j06_230_sparkjob1_0_1.row1Struct.class);
	
	}
}
	
public static class InetAddressSerializer extends com.esotericsoftware.kryo.Serializer<java.net.InetAddress> {

	@Override
	public void write(com.esotericsoftware.kryo.Kryo kryo, com.esotericsoftware.kryo.io.Output output, java.net.InetAddress value) {
		output.writeInt(value.getAddress().length);
		output.writeBytes(value.getAddress());
	}

	@Override
	public java.net.InetAddress read(com.esotericsoftware.kryo.Kryo kryo, com.esotericsoftware.kryo.io.Input input, Class<java.net.InetAddress> paramClass) {
		java.net.InetAddress inetAddress = null;
		try {
			int length = input.readInt();
			byte[] address = input.readBytes(length);
			inetAddress = java.net.InetAddress.getByAddress(address);
		} catch (java.net.UnknownHostException e) {
			// Cannot recreate InetAddress instance : return null
		} catch (com.esotericsoftware.kryo.KryoException e) {
			// Should not happen since write() and read() methods are consistent, but if it does happen, it is an unrecoverable error.
            throw new RuntimeException(e);
		}
		return inetAddress;
	}
}


    public String resuming_logs_dir_path = null;
    public String resuming_checkpoint_path = null;
    public String parent_part_launcher = null;
    public String pid = "0";
    public String rootPid = null;
    public String fatherPid = null;
    public Integer portStats = null;
    public String clientHost;
    public String defaultClientHost = "localhost";
    public String libjars = null;
    private boolean execStat = true;
    public boolean isChildJob = false;
    public String fatherNode = null;
    public String log4jLevel = "";
    public boolean doInspect = false;

    public String contextStr = "zafkir";
    public boolean isDefaultContext = true;

    private java.util.Properties context_param = new java.util.Properties();
    public java.util.Map<String, Object> parentContextMap = new java.util.HashMap<String, Object>();

    public String status= "";

    public static void main(String[] args) throws java.lang.RuntimeException {
            int exitCode = new sparkstreaming_local_basics_j06_230_sparkjob1().runJobInTOS(args);

            if(exitCode == 0) {
                log.info("TalendJob: 'sparkstreaming_local_basics_j06_230_sparkjob1' - Done.");
            } else {
                log.error("TalendJob: 'sparkstreaming_local_basics_j06_230_sparkjob1' - Failed with exit code: " + exitCode + ".");
            }
        if(exitCode == 0) {
            System.exit(exitCode);
        } else {
            throw new java.lang.RuntimeException("TalendJob: 'sparkstreaming_local_basics_j06_230_sparkjob1' - Failed with exit code: " + exitCode + ".");
        }
    }


    public String[][] runJob(String[] args){
            int exitCode = runJobInTOS(args);
            String[][] bufferValue = new String[][] { { Integer.toString(exitCode) } };
        return bufferValue;
    }

    public int runJobInTOS (String[] args) {
        normalizeArgs(args);

        String lastStr = "";
        for (String arg : args) {
            if (arg.equalsIgnoreCase("--context_param")) {
                lastStr = arg;
            } else if (lastStr.equals("")) {
                evalParam(arg);
            } else {
                evalParam(lastStr + " " + arg);
                lastStr = "";
            }
        }

            if(!"".equals(log4jLevel)){
                if("trace".equalsIgnoreCase(log4jLevel)){
                    log.setLevel(org.apache.log4j.Level.TRACE);
                }else if("debug".equalsIgnoreCase(log4jLevel)){
                    log.setLevel(org.apache.log4j.Level.DEBUG);
                }else if("info".equalsIgnoreCase(log4jLevel)){
                    log.setLevel(org.apache.log4j.Level.INFO);
                }else if("warn".equalsIgnoreCase(log4jLevel)){
                    log.setLevel(org.apache.log4j.Level.WARN);
                }else if("error".equalsIgnoreCase(log4jLevel)){
                    log.setLevel(org.apache.log4j.Level.ERROR);
                }else if("fatal".equalsIgnoreCase(log4jLevel)){
                    log.setLevel(org.apache.log4j.Level.FATAL);
                }else if ("off".equalsIgnoreCase(log4jLevel)){
                    log.setLevel(org.apache.log4j.Level.OFF);
                }
                org.apache.log4j.Logger.getRootLogger().setLevel(log.getLevel());
            }
            log.info("TalendJob: 'sparkstreaming_local_basics_j06_230_sparkjob1' - Start.");

        initContext();

        if (clientHost == null) {
            clientHost = defaultClientHost;
        }

        if (pid == null || "0".equals(pid)) {
            pid = TalendString.getAsciiRandomString(6);
        }

        if (rootPid == null) {
            rootPid = pid;
        }
        if (fatherPid == null) {
            fatherPid = pid;
        } else {
            isChildJob = true;
        }


            if (portStats != null) {
                // portStats = -1; //for testing
                if (portStats < 0 || portStats > 65535) {
                    // issue:10869, the portStats is invalid, so this client socket
                    // can't open
                    System.err.println("The statistics socket port " + portStats
                            + " is invalid.");
                    execStat = false;
                }
            } else {
                execStat = false;
            }

            if (execStat) {
                try {
                    runStat.startThreadStat(clientHost, portStats);
                    runStat.setAllPID(rootPid, fatherPid, pid);
                } catch (java.io.IOException ioException) {
                    ioException.printStackTrace();
                }
            }
        String osName = System.getProperty("os.name");
        String snappyLibName = "libsnappyjava.so";
        if(osName.startsWith("Windows")) {
            snappyLibName = "snappyjava.dll";
        } else if(osName.startsWith("Mac")) {
            snappyLibName = "libsnappyjava.jnilib";
        }
        System.setProperty("org.xerial.snappy.lib.name", snappyLibName);
            // Define ctx variable outside of the try, in order to close it on the catch part.
            org.apache.spark.streaming.api.java.JavaStreamingContext ctx = null;
            try {
                // Set job wide SSL settings before creating the Spark context
                setupJobWideSSLConfigurations();
                java.util.Map<String, String> tuningConf = new java.util.HashMap<>();
                org.apache.spark.SparkConf sparkConfiguration = getConf(tuningConf);
								org.apache.spark.sql.SparkSession ss = org.apache.spark.sql.SparkSession.builder().sparkContext(new org.apache.spark.api.java.JavaSparkContext(sparkConfiguration).sc()).getOrCreate();
                                ctx = new org.apache.spark.streaming.api.java.JavaStreamingContext(org.apache.spark.api.java.JavaSparkContext.fromSparkContext(ss.sparkContext()), new org.apache.spark.streaming.Duration(5000));
                                return run(ss, ctx);
            } catch(Exception ex) {
                ex.printStackTrace();
                ex.printStackTrace(errorMessagePS);
                if (ctx != null) {
                    ctx.stop();
                        if (execStat) {
                            runStat.stopThreadStat();
                        }
                }
                this.status = "failure";
                return 1;
            }
    }

        /**
        *
        * This method runs the Spark job using the SparkContext in argument.
        * @param ss, the SparkSession.
        * @param ctx, the JavaStreamingContext.
        * @return the Spark job exit code.
        *
        */
        private int run(org.apache.spark.sql.SparkSession ss, org.apache.spark.streaming.api.java.JavaStreamingContext ctx) throws java.lang.Exception {
                ctx.addStreamingListener(new TalendSparkStreamingListener(ctx.ssc()));

        initContext();

        setContext(ctx.sparkContext().hadoopConfiguration(), ctx.sparkContext());

        if (doInspect) {
            System.out.println("== inspect start ==");
            System.out.println("{");
                System.out.println("  \"SPARK_MASTER\": \"" + ctx.sparkContext().getConf().get("spark.master") + "\",");
                System.out.println("  \"SPARK_UI_PORT\": \"" + ctx.sparkContext().getConf().get("spark.ui.port", "4040") + "\",");
                System.out.println("  \"JOB_NAME\": \"" + ctx.sparkContext().getConf().get("spark.app.name", jobName) + "\"");
                System.out.println("}"); //$NON-NLS-1$
                System.out.println("== inspect end ==");
            }
            globalMap = new GlobalVar(ctx.sparkContext().hadoopConfiguration());
                        tKafkaInput_1Process(ss, ctx, globalMap);
            ctx.start();
                        ctx.awaitTermination();


                    tKafkaInput_1PostProcess(ctx, globalMap);
        return 0;
    }

    /**
     *
     * This method has the responsibility to return a Spark configuration for the Spark job to run.
     * @return a Spark configuration.
     *
     */
    private org.apache.spark.SparkConf getConf(java.util.Map<String, String> tuningConf) throws java.lang.Exception {
        org.apache.spark.SparkConf sparkConfiguration = new org.apache.spark.SparkConf();
        sparkConfiguration.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
        sparkConfiguration.set("spark.kryo.registrator", TalendKryoRegistrator.class.getName());

        java.text.DateFormat outdfm = new java.text.SimpleDateFormat("yyyy_MM_dd_HH_mm_ss");
        String appName = projectName + "_" + jobName + "_" + jobVersion + "_" + outdfm.format(new java.util.Date());
        sparkConfiguration.setAppName(appName);
            log.info("Created App:" + appName);
            sparkConfiguration.setMaster("local[*]");

            routines.system.GetJarsToRegister getJarsToRegister = new routines.system.GetJarsToRegister();
            java.util.List<String> listJar = new java.util.ArrayList<String>();
            String libJarUriPrefix = getLibJarURIPrefix();
            if(libjars != null) {
                for(String jar:libjars.split(",")) {
                            listJar.add(getJarsToRegister.replaceJarPaths(jar, libJarUriPrefix, true));
                }
            }


				routines.system.BigDataUtil.installWinutils(context.local_tmp_dir, getJarsToRegister.replaceJarPaths("../lib/winutils-hadoop-2.6.0.exe"));
            sparkConfiguration.setJars(listJar.toArray(new String[listJar.size()]));
                tuningConf.put("spark.ui.port", "4040");
            tuningConf.put("spark.executor.memory", "512m");
            sparkConfiguration.set("spark.sql.warehouse.dir", "file:///" +  context.local_tmp_dir + "/spark-warehouse");

	
	
        sparkConfiguration.setAll(scala.collection.JavaConversions.mapAsScalaMap(tuningConf));
                sparkConfiguration.set("spark.local.dir", context.local_tmp_dir);


        return sparkConfiguration;
    }
    private void setupJobWideSSLConfigurations(){
            // No SSL configurations required
    }

    private String genTempFolderForComponent(String name) {
        java.io.File tempDir = new java.io.File("/tmp/" + pid, name);
        String tempDirPath = tempDir.getPath();
        if (java.io.File.separatorChar != '/')
            tempDirPath = tempDirPath.replace(java.io.File.separatorChar, '/');
        return tempDirPath;
    }

    private void initContext(){
        //get context
        try{
            //call job/subjob with an existing context, like: --context=production. if without this parameter, there will use the default context instead.
            java.io.InputStream inContext = sparkstreaming_local_basics_j06_230_sparkjob1.class.getClassLoader().getResourceAsStream("local_project/sparkstreaming_local_basics_j06_230_sparkjob1_0_1/contexts/"+contextStr+".properties");
            if(isDefaultContext && inContext == null){

            }else{
                if(inContext!=null){
                    //defaultProps is in order to keep the original context value
                    defaultProps.load(inContext);
                    inContext.close();
                    context = new ContextProperties(defaultProps);
                }else{
                    //print info and job continue to run, for case: context_param is not empty.
                    System.err.println("Could not find the context " + contextStr);
                }
            }

            if(!context_param.isEmpty()){
                context.putAll(context_param);
            }
            context.loadValue(context_param,null);
            if(parentContextMap != null && !parentContextMap.isEmpty()){
                    if(parentContextMap.containsKey("kafka_broker_host")){
                        context.kafka_broker_host = (String) parentContextMap.get("kafka_broker_host");
                    }
                    if(parentContextMap.containsKey("kafka_broker_port")){
                        context.kafka_broker_port = (String) parentContextMap.get("kafka_broker_port");
                    }
                    if(parentContextMap.containsKey("kafka_topic")){
                        context.kafka_topic = (String) parentContextMap.get("kafka_topic");
                    }
                    if(parentContextMap.containsKey("result_table")){
                        context.result_table = (String) parentContextMap.get("result_table");
                    }
                    if(parentContextMap.containsKey("data_output_dir")){
                        context.data_output_dir = (String) parentContextMap.get("data_output_dir");
                    }
                    if(parentContextMap.containsKey("result_port")){
                        context.result_port = (String) parentContextMap.get("result_port");
                    }
                    if(parentContextMap.containsKey("result_username")){
                        context.result_username = (String) parentContextMap.get("result_username");
                    }
                    if(parentContextMap.containsKey("local_tmp_dir")){
                        context.local_tmp_dir = (String) parentContextMap.get("local_tmp_dir");
                    }
                    if(parentContextMap.containsKey("param_file_path")){
                        context.param_file_path = (String) parentContextMap.get("param_file_path");
                    }
                    if(parentContextMap.containsKey("result_host")){
                        context.result_host = (String) parentContextMap.get("result_host");
                    }
                    if(parentContextMap.containsKey("result_database")){
                        context.result_database = (String) parentContextMap.get("result_database");
                    }
                    if(parentContextMap.containsKey("result_password")){
                        context.result_password = (String) parentContextMap.get("result_password");
                    }
                    if(parentContextMap.containsKey("data_dir")){
                        context.data_dir = (String) parentContextMap.get("data_dir");
                    }
            }
        }catch (java.io.IOException ie){
            System.err.println("Could not load context "+contextStr);
            ie.printStackTrace();
        }
    }

    private void setContext(Configuration conf, org.apache.spark.api.java.JavaSparkContext ctx){
        //get context
        //call job/subjob with an existing context, like: --context=production. if without this parameter, there will use the default context instead.
        java.net.URL inContextUrl = sparkstreaming_local_basics_j06_230_sparkjob1.class.getClassLoader().getResource("local_project/sparkstreaming_local_basics_j06_230_sparkjob1_0_1/contexts/"+contextStr+".properties");
        if(isDefaultContext && inContextUrl == null){

        }else{
            if(inContextUrl!=null){
                conf.set(ContextProperties.CONTEXT_FILE_NAME, contextStr+".properties");

            }
        }

        if(!context_param.isEmpty()){
            for(Object contextKey : context_param.keySet()){
                conf.set(ContextProperties.CONTEXT_PARAMS_PREFIX + contextKey, context.getProperty(contextKey.toString()));
                conf.set(ContextProperties.CONTEXT_KEYS, conf.get(ContextProperties.CONTEXT_KEYS, "") + " " + contextKey);
            }
        }

        if(parentContextMap != null && !parentContextMap.isEmpty()){
                if(parentContextMap.containsKey("kafka_broker_host")){
                    conf.set(ContextProperties.CONTEXT_PARENT_PARAMS_PREFIX + "kafka_broker_host", parentContextMap.get("kafka_broker_host").toString());
                    conf.set(ContextProperties.CONTEXT_PARENT_KEYS, conf.get(ContextProperties.CONTEXT_KEYS, "") + " " + "kafka_broker_host");
                }
                if(parentContextMap.containsKey("kafka_broker_port")){
                    conf.set(ContextProperties.CONTEXT_PARENT_PARAMS_PREFIX + "kafka_broker_port", parentContextMap.get("kafka_broker_port").toString());
                    conf.set(ContextProperties.CONTEXT_PARENT_KEYS, conf.get(ContextProperties.CONTEXT_KEYS, "") + " " + "kafka_broker_port");
                }
                if(parentContextMap.containsKey("kafka_topic")){
                    conf.set(ContextProperties.CONTEXT_PARENT_PARAMS_PREFIX + "kafka_topic", parentContextMap.get("kafka_topic").toString());
                    conf.set(ContextProperties.CONTEXT_PARENT_KEYS, conf.get(ContextProperties.CONTEXT_KEYS, "") + " " + "kafka_topic");
                }
                if(parentContextMap.containsKey("result_table")){
                    conf.set(ContextProperties.CONTEXT_PARENT_PARAMS_PREFIX + "result_table", parentContextMap.get("result_table").toString());
                    conf.set(ContextProperties.CONTEXT_PARENT_KEYS, conf.get(ContextProperties.CONTEXT_KEYS, "") + " " + "result_table");
                }
                if(parentContextMap.containsKey("data_output_dir")){
                    conf.set(ContextProperties.CONTEXT_PARENT_PARAMS_PREFIX + "data_output_dir", parentContextMap.get("data_output_dir").toString());
                    conf.set(ContextProperties.CONTEXT_PARENT_KEYS, conf.get(ContextProperties.CONTEXT_KEYS, "") + " " + "data_output_dir");
                }
                if(parentContextMap.containsKey("result_port")){
                    conf.set(ContextProperties.CONTEXT_PARENT_PARAMS_PREFIX + "result_port", parentContextMap.get("result_port").toString());
                    conf.set(ContextProperties.CONTEXT_PARENT_KEYS, conf.get(ContextProperties.CONTEXT_KEYS, "") + " " + "result_port");
                }
                if(parentContextMap.containsKey("result_username")){
                    conf.set(ContextProperties.CONTEXT_PARENT_PARAMS_PREFIX + "result_username", parentContextMap.get("result_username").toString());
                    conf.set(ContextProperties.CONTEXT_PARENT_KEYS, conf.get(ContextProperties.CONTEXT_KEYS, "") + " " + "result_username");
                }
                if(parentContextMap.containsKey("local_tmp_dir")){
                    conf.set(ContextProperties.CONTEXT_PARENT_PARAMS_PREFIX + "local_tmp_dir", parentContextMap.get("local_tmp_dir").toString());
                    conf.set(ContextProperties.CONTEXT_PARENT_KEYS, conf.get(ContextProperties.CONTEXT_KEYS, "") + " " + "local_tmp_dir");
                }
                if(parentContextMap.containsKey("param_file_path")){
                    conf.set(ContextProperties.CONTEXT_PARENT_PARAMS_PREFIX + "param_file_path", parentContextMap.get("param_file_path").toString());
                    conf.set(ContextProperties.CONTEXT_PARENT_KEYS, conf.get(ContextProperties.CONTEXT_KEYS, "") + " " + "param_file_path");
                }
                if(parentContextMap.containsKey("result_host")){
                    conf.set(ContextProperties.CONTEXT_PARENT_PARAMS_PREFIX + "result_host", parentContextMap.get("result_host").toString());
                    conf.set(ContextProperties.CONTEXT_PARENT_KEYS, conf.get(ContextProperties.CONTEXT_KEYS, "") + " " + "result_host");
                }
                if(parentContextMap.containsKey("result_database")){
                    conf.set(ContextProperties.CONTEXT_PARENT_PARAMS_PREFIX + "result_database", parentContextMap.get("result_database").toString());
                    conf.set(ContextProperties.CONTEXT_PARENT_KEYS, conf.get(ContextProperties.CONTEXT_KEYS, "") + " " + "result_database");
                }
                if(parentContextMap.containsKey("result_password")){
                    conf.set(ContextProperties.CONTEXT_PARENT_PARAMS_PREFIX + "result_password", parentContextMap.get("result_password").toString());
                    conf.set(ContextProperties.CONTEXT_PARENT_KEYS, conf.get(ContextProperties.CONTEXT_KEYS, "") + " " + "result_password");
                }
                if(parentContextMap.containsKey("data_dir")){
                    conf.set(ContextProperties.CONTEXT_PARENT_PARAMS_PREFIX + "data_dir", parentContextMap.get("data_dir").toString());
                    conf.set(ContextProperties.CONTEXT_PARENT_KEYS, conf.get(ContextProperties.CONTEXT_KEYS, "") + " " + "data_dir");
                }
        }
    }


    /**
     * @return the -libjars elements prefix depending on the OS.
     */
    private static String getLibJarURIPrefix() {
        return System.getProperty("os.name").startsWith("Windows") ? "file:///" : "file://";
    }


    private void evalParam(String arg) {
        if (arg.startsWith("--resuming_logs_dir_path")) {
            resuming_logs_dir_path = arg.substring(25);
        } else if (arg.startsWith("--resuming_checkpoint_path")) {
            resuming_checkpoint_path = arg.substring(27);
        } else if (arg.startsWith("--parent_part_launcher")) {
            parent_part_launcher = arg.substring(23);
        } else if (arg.startsWith("--father_pid=")) {
            fatherPid = arg.substring(13);
        } else if (arg.startsWith("--root_pid=")) {
            rootPid = arg.substring(11);
        } else if (arg.startsWith("--pid=")) {
            pid = arg.substring(6);
        } else if (arg.startsWith("--context=")) {
            contextStr = arg.substring("--context=".length());
            isDefaultContext = false;
        } else if (arg.startsWith("--context_param")) {
            String keyValue = arg.substring("--context_param".length() + 1);
            int index = -1;
            if (keyValue != null && (index = keyValue.indexOf('=')) > -1) {
                context_param.put(keyValue.substring(0, index), replaceEscapeChars(keyValue.substring(index + 1)));
            }
        } else if (arg.startsWith("--stat_port=")) {
            String portStatsStr = arg.substring(12);
            if (portStatsStr != null && !portStatsStr.equals("null")) {
                portStats = Integer.parseInt(portStatsStr);
            }
        } else if (arg.startsWith("--client_host=")) {
            clientHost = arg.substring(14);
        } else if (arg.startsWith("--log4jLevel=")) {
            log4jLevel = arg.substring(13);
        } else if (arg.startsWith("--inspect")) {
            doInspect = Boolean.valueOf(arg.substring("--inspect=".length()));
        }
    }

    private void normalizeArgs(String[] args){
        java.util.List<String> argsList = java.util.Arrays.asList(args);
        int indexlibjars = argsList.indexOf("-libjars") + 1;
        libjars = indexlibjars == 0 ? null : argsList.get(indexlibjars);
    }

    private final String[][] escapeChars = {
        {"\\\\","\\"},{"\\n","\n"},{"\\'","\'"},{"\\r","\r"},
        {"\\f","\f"},{"\\b","\b"},{"\\t","\t"}
        };
    private String replaceEscapeChars (String keyValue) {

        if (keyValue == null || ("").equals(keyValue.trim())) {
            return keyValue;
        }

        StringBuilder result = new StringBuilder();
        int currIndex = 0;
        while (currIndex < keyValue.length()) {
            int index = -1;
            // judege if the left string includes escape chars
            for (String[] strArray : escapeChars) {
                index = keyValue.indexOf(strArray[0],currIndex);
                if (index>=0) {

                    result.append(keyValue.substring(currIndex, index + strArray[0].length()).replace(strArray[0], strArray[1]));
                    currIndex = index + strArray[0].length();
                    break;
                }
            }
            // if the left string doesn't include escape chars, append the left into the result
            if (index < 0) {
                result.append(keyValue.substring(currIndex));
                currIndex = currIndex + keyValue.length();
            }
        }

        return result.toString();
    }

    public String getStatus() {
        return status;
    }


}