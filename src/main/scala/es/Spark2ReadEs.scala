package es

import org.apache.spark.{SparkContext, SparkConf}
import org.elasticsearch.spark._

import scala.collection.Map
import org.apache.log4j.Logger

/**
  *
  *
  * <pre>
  * User: liuyu
  * Date: 2016/9/8
  * Time: 14:08
  * </pre>
  *
  * @author liuyu
  */
class Spark2ReadEs {

}

object Spark2ReadEs extends App{
	private[this] val LOG = Logger.getLogger(getClass().getName())

	val conf: SparkConf = new SparkConf().setMaster("local").setAppName("Spark2ReadEs")
	conf.set("es.index.auto.create", "true")   //设置es自动创建索引
	conf.set("es.nodes","192.168.1.107")   //设置连接的es集群节点
	conf.set("es.port","9200")  //端口号

	val sc = new SparkContext(conf)
	val collect: Array[(String, Map[String, AnyRef])] = sc.esRDD("spark/docs","?q=*").collect()
	for(li <- collect){
		LOG.info(li)
	}
}
