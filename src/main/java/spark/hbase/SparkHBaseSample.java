package spark.hbase;

import java.net.URL;
import java.net.URLClassLoader;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import com.cloudera.spark.hbase.JavaHBaseContext;

import scala.Tuple2;
import scala.Tuple3;

public class SparkHBaseSample {
    public static void main(String args[]) {
        args = new String[]{"spark://inblr-sravank.local:7077", "test"};
        if (args.length == 0) {
            System.out
                    .println("JavaHBaseDistributedScan  {master} {tableName}");
        }

//        printClasspath();

        String master = args[0];
        String tableName = args[1];

        JavaSparkContext jsc = new JavaSparkContext(master,
                "JavaHBaseDistributedScan");
//        jsc.addJar("file:///Users/sravank/Career/Samples/libraries/spark-core_2.10-1.5.2.jar");

        Configuration conf = HBaseConfiguration.create();
        //conf.addResource(new Path("/etc/hbase/conf/core-site.xml"));
        conf.addResource(new Path("/Users/sravank/Softwares/hadoop-2.6.0/etc/hadoop/core-site.xml"));
//    conf.addResource(new Path("/etc/hbase/conf/hbase-site.xml"));
        conf.addResource(new Path("/Users/sravank/Career/BigData/hbase-1.1.4/conf/hbase-site.xml"));

        JavaHBaseContext hbaseContext = new JavaHBaseContext(jsc, conf);

        Scan scan = new Scan();
        scan.setCaching(100);

        JavaRDD<Tuple2<byte[], List<Tuple3<byte[], byte[], byte[]>>>> javaRdd = hbaseContext.hbaseRDD(tableName, scan);

        List<Tuple2<byte[], List<Tuple3<byte[], byte[], byte[]>>>> results = javaRdd.collect();

        results.size();
    }

    private static void printClasspath() {
        ClassLoader cl = ClassLoader.getSystemClassLoader();

        URL[] urls = ((URLClassLoader) cl).getURLs();

        for (URL url : urls) {
            System.out.println(url.getFile());
        }
    }
}
