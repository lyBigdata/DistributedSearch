package solr;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import hbase.ConHbaseServices;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.HttpSolrServer;
import org.apache.solr.common.SolrInputDocument;

/**
 * 读取hbase表数据,在solr中创建索引
 *
 * User: liuyu
 * Date: 2016/8/29
 * Time: 10:31
 * </pre>
 *
 * @author liuyu
 */
public class SolrIndexer {

    private String[] index = null;   //需要在solr中创建索引的字段
    private String url = null;     //solr服务的url
    private HttpSolrServer solrServer = null;  //solr服务实例

    protected static Log LOG = LogFactory.getLog(SolrIndexer.class);

    /**
     * 构造函数
     * @param pathName  配置文件路径及名称
     */
    public SolrIndexer(String pathName) {

        Properties pro = getPro(pathName);
        if(pro.getProperty("solrUrl").equals("")){
            LOG.error("配置文件中solr服务的url为空...");
            System.exit(0);
        }else {
            this.url = pro.getProperty("solrUrl");
        }

        if(pro.getProperty("indexFields").equals("")){
            LOG.error("配置文件中索引字段配置为空...");
            System.exit(0);
        }else {
            this.index = pro.getProperty("indexFields").split(",");
        }

        this.solrServer = new HttpSolrServer(this.url);
    }


    /**
     * 加载properties文件
     * @param pathName
     * @return
     */
    public static Properties getPro(String pathName) {
        Properties props = new Properties();

        ClassLoader classLoader = null;
        try {
            classLoader = Thread.currentThread().getContextClassLoader();
            if (null == classLoader) {
                classLoader = SolrIndexer.class.getClassLoader();
            }
            InputStream inputStream = classLoader.getResourceAsStream(pathName);

            props.load(inputStream);

        } catch (Exception e) {
            e.printStackTrace();
        }

        return props;
    }


    /**
     * 从hbase中读取数据,在solr中创建索引(目前只支持从一个列族中读取数据,创建索引)
     * @param tableName  表名称
     * @param columnFamily  列族
     * @throws SolrServerException
     * @throws IOException
     */
    public void createIndexsFromHbaseReadData(String tableName,String columnFamily) throws  SolrServerException,IOException{
        ResultScanner result=null;

        LOG.info("start read hbase data...");
        try{
            result= ConHbaseServices.getAllByColumnFamily(tableName, columnFamily);
        }catch (Exception e){
            LOG.error("read hbase data error : "+e.getMessage());
        }

        LOG.info("start create index...");

        int i = 0;
        if(result != null){
            try {
                for (Result r : result) {
                    SolrInputDocument solrDoc = new SolrInputDocument();
                    solrDoc.addField("rowkey", new String(r.getRow()));
                    for (KeyValue kv : r.raw()) {
                        String fieldName = new String(kv.getQualifier());
                        String fieldValue = new String(kv.getValue());

                        for (int j = 0;j < index.length;j++){
                            if(fieldName.equalsIgnoreCase(index[j])){
                                solrDoc.addField(fieldName, fieldValue);
                                break;
                            }
                        }
                    }
                    this.solrServer.add(solrDoc);
                    this.solrServer.commit(true, true, true);
                    i++;
                    LOG.info("已经成功处理 " + i + " 条数据...");
                }
                result.close();
                LOG.info("索引数据创建完成...");
            } catch (IOException e) {
                LOG.error(e.getMessage());
            } finally {
                result.close();
                LOG.error("处理数据出错...");
            }
        }else {
            LOG.info("未读取到数据...");
        }
    }


    /**
     * @param args
     * @throws IOException
     * @throws SolrServerException
     */
    public static void main(String[] args) throws IOException,
            SolrServerException {

    }
}