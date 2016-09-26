package com.test

import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.io._
import org.apache.hadoop.hbase.mapreduce._
import org.apache.hadoop.mapreduce._
import org.apache.hadoop.hbase._
import org.apache.spark._
import org.apache.hadoop.hbase.util._
import org.apache.hadoop.fs._

/**
 * Created by Honda on 2016/9/26.
 */
object BulkLoadExecuter
{
    def main(args: Array[String])
    {
        val conf = HBaseConfiguration.create()
        val tableName = "testtable123"
        val connection = ConnectionFactory.createConnection(conf)
        val hbaseAdmin = connection.getAdmin
        val table = (connection.getTable( TableName.valueOf(tableName))).asInstanceOf[HTable]

        val job = Job.getInstance(conf)
        job.setMapOutputKeyClass( classOf[ImmutableBytesWritable])
        job.setMapOutputValueClass( classOf[KeyValue])
        HFileOutputFormat2.configureIncrementalLoadMap(job,table)
        val regionLocator = connection.getRegionLocator(TableName.valueOf(tableName))
        HFileOutputFormat2.configureIncrementalLoad(job,table,regionLocator)

        val spConf = new SparkConf().setAppName("Bulk Load")
        val sc = new SparkContext(spConf)
        val num = sc.parallelize( 1 to 10)
        val rdd = num.map(x => {
            val kv:KeyValue = new KeyValue( Bytes.toBytes("46697" + x) , "cf1".getBytes() , "name".getBytes() , Bytes.toBytes("name" + x))
            (new ImmutableBytesWritable(Bytes.toBytes("46697" + x)),kv)

        })

        // save HFILE on HDFS
        rdd.saveAsNewAPIHadoopFile("bulk_load_tmp/xxx" , classOf[ImmutableBytesWritable] , classOf[KeyValue] , classOf[HFileOutputFormat2] ,conf)

        //Bulk load HFILE to HBase
        val bulkLoader = new LoadIncrementalHFiles(conf)
        bulkLoader.doBulkLoad(new Path("bulk_load_tmp/xxx") ,  table )

    }
}
