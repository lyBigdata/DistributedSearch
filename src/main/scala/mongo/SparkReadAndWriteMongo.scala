package mongo

import org.apache.hadoop.conf.Configuration
import org.apache.spark.{SparkContext, SparkConf}
import org.bson.BasicBSONObject

/**
  *
  *
  * <pre>
  * User: liuyu
  * Date: 2016/8/30
  * Time: 9:14
  * </pre>
  *
  * @author liuyu
  */
object SparkReadAndWriteMongo {
	def main(args: Array[String]) {
		val conf = new SparkConf().setAppName("SparkRDDCount").setMaster("local")
		val sc = new SparkContext(conf);
		val data = sc.parallelize(List(("Tom", 31), ("Jack", 22), ("Mary", 25)))
		val config = new Configuration()
		//spark.oc指的是，spark是MongoDB的数据库的名字，而ic表示数据库中的一个Collection
		config.set("mongo.input.uri", "mongodb://192.168.1.115:9999/spark.ic")
		config.set("mongo.output.uri", "mongodb://192.168.1.115:9999/spark.oc")
		//使用MongoOutputFormat将数据写入到MongoDB中
		val rdd = data.map((elem) => {
			val obj = new BasicBSONObject()
			obj.put("name", elem._1)
			//转换后的结果，KV对
			//第一个是BSON的ObjectId，插入时可以指定为null，MongoDB Driver在插入到MongoDB时，自动的生成
			//obj是BSONObject，是MongoDB Driver接收的插入对象
			(null, obj)
		})
		//RDD data is a KV pair,so it can use saveAsNewAPIHadoopFile
		rdd.saveAsNewAPIHadoopFile("file:///bogus", classOf[Any], classOf[Any], classOf[com.mongodb.hadoop.MongoOutputFormat[Any, Any]], config)
	}

}
