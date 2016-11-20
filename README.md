# hbase-geoip
Goto HBase shell:

	hbase shell
	
create HBase table as:

	 create 'iptable', 'ipcols'
	 put 'iptable', 'row1', 'ipcols:ip', '216.58.217.46'
	 put 'iptable', 'row1', 'ipcols:city', ' '

Run the Job as:

	 runuser -l hdfs -c 'spark-submit --class io.woolford.Main /tmp/hbase-geoip-1.0-SNAPSHOT.jar'
	 
Goto HBase shell:

	scan 'iptable'
	
Result will be something like:

	hbase(main):001:0> scan 'iptable'
	ROW                                   COLUMN+CELL
 	row1                                 column=ipcols:city, timestamp=1479634773590, value=Mountain View
 	row1                                 column=ipcols:ip, timestamp=1479540100667, value=216.58.217.46
	1 row(s) in 0.2620 seconds

