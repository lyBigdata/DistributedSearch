package es

import java.util.Date

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkContext, SparkConf}
import org.elasticsearch.spark._
import org.elasticsearch.spark.rdd.EsSpark

/**
  *
  *
  * <pre>
  * User: liuyu
  * Date: 2016/9/8
  * Time: 9:56
  * </pre>
  *
  * @author liuyu
  */
class Spark2WriteEs {

}

object Spark2WriteEs extends App{

	//初始化配置文件,并且设置es相关参数
	val conf = new SparkConf().setAppName("Spark2WriteEs").setMaster("local")
	conf.set("es.index.auto.create", "true")   //设置es自动创建索引
	conf.set("es.nodes","192.168.1.107")   //设置连接的es集群节点
	conf.set("es.port","9200")  //端口号

	val sc: SparkContext = new SparkContext(conf)

	/*var se = collection.mutable.Seq[Map[String, Any]]()

	val map = Map("name" -> "zhangsan", "age" -> "20", "time" -> new Date())
	se = se :+ (map)

	sc.makeRDD(se).saveToEs("index/docs")*/

	val textFile = sc.textFile("F:/Work/DistributedSearch/src/main/resources/lppz-act.txt",4).
			flatMap(a=>{
				var seq = collection.mutable.Seq[Map[String, Any]]()
				val splits = a.split("\\s+")
				if(splits.length==4){
					val map = Map("cookieId" -> splits(0),
								"actionType" -> splits(1),
								"random" -> splits(2),
								"time" -> splits(3))
					seq = seq :+ map
				}
				seq
			})

	EsSpark.saveToEs(textFile, "spark/docs");
}
