package io.woolford;

import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;
import java.net.InetAddress;
import java.util.Arrays;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos;
import org.apache.hadoop.hbase.util.Base64;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.Job;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;

import com.maxmind.geoip2.DatabaseReader;
import com.maxmind.geoip2.exception.GeoIp2Exception;
import com.maxmind.geoip2.model.CityResponse;

import scala.Tuple2;

public class Main implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = -8435845710016739909L;

	static Logger logger = Logger.getLogger(Main.class.getName());

	//hbase schema definition
	private static final String tableName = "iptable";
	private static final String columnFamily = "ipcols";
	private static final String ipCol = "ip";
	private static final String cityCol = "city";

	public static void main(String[] args) throws IOException, GeoIp2Exception {
		new Main().start();
	}

	public void start() {
		SparkConf sparkConf = new SparkConf();
		sparkConf.setAppName("hbase-geoip");
		// sparkConf.setMaster("local[*]"); // comment this out when running via `spark-submit` on a cluster
		
		JavaSparkContext sc = new JavaSparkContext(sparkConf);
		Configuration conf = HBaseConfiguration.create();

		Scan scan = new Scan();
		scan.addFamily(Bytes.toBytes(columnFamily));
		scan.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(ipCol));
		scan.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(cityCol));

		try {
			conf.set(TableInputFormat.INPUT_TABLE, tableName);
			conf.set(TableOutputFormat.OUTPUT_TABLE, tableName);
			conf.set(TableInputFormat.SCAN, convertScanToString(scan));

			JavaPairRDD<ImmutableBytesWritable, Result> hBaseRDD = sc.newAPIHadoopRDD(conf, TableInputFormat.class,
					ImmutableBytesWritable.class, Result.class);

			//read from hbase
			JavaRDD<String> ips = hBaseRDD.flatMap(new FlatMapFunction<Tuple2<ImmutableBytesWritable, Result>, String>() {
						private static final long serialVersionUID = 1L;

						@Override
						public Iterable<String> call(Tuple2<ImmutableBytesWritable, Result> t) throws Exception {
							String ip = Bytes.toString(t._2.getValue(Bytes.toBytes(columnFamily), Bytes.toBytes(ipCol)));
							String city = Bytes.toString(t._2.getValue(Bytes.toBytes(columnFamily), Bytes.toBytes(cityCol)));
							logger.info("IP: " + ip + " City: " + city);
							return Arrays.asList(ip);
						}
					});

			//iterate through values.. can be remvoed.
			ips.foreach(new VoidFunction<String>() {
				private static final long serialVersionUID = 1L;

				@Override
				public void call(String ip) throws Exception {
					// sample code to lookup geoip data for an IP address
					DatabaseReader reader = getDatabaseReader();

					InetAddress ipAddress = InetAddress.getByName(ip);

					CityResponse response = reader.city(ipAddress);

					logger.info(response.getCity().getName());
				}
			});

			// new Hadoop API configuration
			Job newAPIJobConfiguration = Job.getInstance(conf);
			newAPIJobConfiguration.getConfiguration().set(TableOutputFormat.OUTPUT_TABLE, tableName);
			newAPIJobConfiguration.setOutputFormatClass(org.apache.hadoop.hbase.mapreduce.TableOutputFormat.class);
			
			// write to hbase.
			JavaPairRDD<ImmutableBytesWritable, Put> hbasePuts = ips
					.mapToPair(new PairFunction<String, ImmutableBytesWritable, Put>() {
						
						private static final long serialVersionUID = 1L;

						@Override
						public Tuple2<ImmutableBytesWritable, Put> call(String ip) throws Exception {
							DatabaseReader reader = getDatabaseReader();
							InetAddress ipAddress = InetAddress.getByName(ip);
							CityResponse response = reader.city(ipAddress);
							logger.info("saving City: " + response.getCity().getName());
							
							// update the CITY using GeoIP
							Put put = new Put(Bytes.toBytes("row1"));
							put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(cityCol), Bytes.toBytes(response.getCity().getName()));

							return new Tuple2<ImmutableBytesWritable, Put>(new ImmutableBytesWritable(), put);
						}
					});

			hbasePuts.saveAsNewAPIHadoopDataset(newAPIJobConfiguration.getConfiguration());
			
		} catch (Exception e) {
			logger.log(Level.WARNING, "", e);
			;
		} finally {
			sc.close();
		}
	}

	private String convertScanToString(Scan scan) throws IOException {
		ClientProtos.Scan proto = ProtobufUtil.toScan(scan);
		return Base64.encodeBytes(proto.toByteArray());
	}

	private DatabaseReader getDatabaseReader() throws IOException {
		DatabaseReader reader = null;
		if (reader == null) {
			synchronized (this) {
				if (reader == null) {
					InputStream database = this.getClass().getClassLoader().getResourceAsStream("geoip/GeoIP2-City.mmdb");
					reader = new DatabaseReader.Builder(database).build();
				}
			}
		}
		return reader;
	}

}
