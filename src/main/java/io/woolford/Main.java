package io.woolford;

import com.maxmind.geoip2.DatabaseReader;
import com.maxmind.geoip2.exception.GeoIp2Exception;
import com.maxmind.geoip2.model.CityResponse;
import org.apache.commons.io.FileUtils;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.util.logging.Logger;

public class Main {

    static Logger logger = Logger.getLogger(Main.class.getName());

    public static void main(String[] args) throws IOException, GeoIp2Exception {

        SparkConf conf = new SparkConf();
        conf.setAppName("hbase-geoip");
        conf.setMaster("local[*]"); // comment this out when running via `spark-submit` on a cluster

        // create spark context
        SparkContext sc = new SparkContext(conf);
        // TODO: read from HBase to spark dataframe, augment IP address, write back to HBase
        
        // sample code to lookup geoip data for an IP address
        DatabaseReader reader = getDatabaseReader();
        InetAddress ipAddress = InetAddress.getByName("216.58.217.46");

        CityResponse response = reader.city(ipAddress);

        logger.info(response.toString());

    }

    private static DatabaseReader getDatabaseReader() throws IOException {
        File database = FileUtils.getFile("src/main/resources/GeoIP2-City.mmdb");
        DatabaseReader reader = new DatabaseReader.Builder(database).build();
        return reader;

    }

}
