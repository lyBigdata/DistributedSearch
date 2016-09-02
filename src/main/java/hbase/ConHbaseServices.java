package hbase;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/**
 * <pre>
 * User: liuyu
 * Date: 2016/8/25
 * Time: 10:51
 * </pre>
 *
 * @author liuyu
 */
public class ConHbaseServices {

    protected static Log LOG = LogFactory.getLog(ConHbaseServices.class);

    //java中首先是静态块先执行，静态方法，最后是构造函数
    //声明静态配置
    private static Configuration conf=null;

    static {
        Properties properties = new Properties();
        try{
            properties.load(ClassLoader.getSystemResourceAsStream("hbaseconfig.properties"));
        }catch (IOException io){
            LOG.error(io.getMessage());
        }
        conf= HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", properties.getProperty("hbase.zookeeper.quorum"));
        conf.set("hbase.zookeeper.property.clientPort", properties.getProperty("hbase.zookeeper.property.clientPor"));
    }

    /**
     *判断表是否存在
     * @param tableName   表名称
     * @return    返回 true 和 false
     * @throws IOException
     */
    public static boolean tableIsExist(String tableName) throws IOException {
        HBaseAdmin admin = new HBaseAdmin(conf);
        return  admin.tableExists(tableName);
    }

    /**
     * 删除表
     * @param tableName  表名称
     * @throws IOException
     */
    public   static  void delTable(String tableName) throws  IOException{
        HBaseAdmin admin = new HBaseAdmin(conf);
        if(admin.tableExists(tableName)){
            admin.disableTable(tableName);
            admin.deleteTable(tableName);
            System.out.println("删除表成功。。。"+tableName);
            LOG.info("删除表成功。。。"+tableName);
        }else {
            System.out.println("删除表失败，表不存在。。。"+tableName);
            LOG.info("删除表失败，表不存在。。。"+tableName);
            System.exit(0);
        }
    }


    /**
     *  创建表
     * @param tableName  表名称
     * @param columnFamilys   要创建表的列族
     * @throws Exception
     */
    public static void createTable(String tableName, String[] columnFamilys)
            throws Exception {
        //新建hbase数据库操作对象
        HBaseAdmin admin = new HBaseAdmin(conf);
        //初始化一个表的描述
        HTableDescriptor desc = new HTableDescriptor(tableName);
        if(admin.tableExists(tableName)){
            System.out.println("创建表失败，表已经存在。。。"+tableName);
            LOG.info("创建表失败，表已经存在。。。"+tableName);
            System.exit(0);
        }else {
            //表描述添加列族
            for (String columnFamily : columnFamilys) {
                desc.addFamily(new HColumnDescriptor(columnFamily));
            }
            admin.createTable(desc);
            System.out.println("创建表成功。。。"+tableName);
            LOG.info("创建表成功。。。"+tableName);
        }
    }


    /**
     * 添加一条记录
     * @param tableName  表名称
     * @param rowKey 行健
     * @param columnFamily 列族
     * @param column  列
     * @param value  值
     * @throws IOException
     */
    public  static void addOneRecode(String tableName,String rowKey,String columnFamily,String column,String value) throws  IOException{
        HTable table = new HTable(conf, tableName);
        Put put = new Put(Bytes.toBytes(rowKey));// 指定行
        //增加列族，列，值
        put.add(Bytes.toBytes(columnFamily),Bytes.toBytes(column),Bytes.toBytes(value));
        table.put(put);
        table.close();
    }

    /**
     *  根据rowKey查询一条记录
     * @param tableName  表名称
     * @param rowKey  行健
     * @return 返回结果
     * @throws IOException
     */
    public  static  Result getByRowkey(String tableName,String rowKey) throws  IOException{
        HTable table = new HTable(conf, tableName);
        Get get = new Get(Bytes.toBytes(rowKey));
        Result result=null;
        try{
            result = table.get(get);
        }catch (IOException io){
            LOG.error(io.getMessage());
        }

        return  result;
    }



    /**
     * 删除一条(行)数据
     * @param tableName   表名称
     * @param row  行健
     * @throws Exception
     */
    public static void delRow(String tableName, String row) throws Exception {
        HTable table = new HTable(conf, tableName);
        Delete del = new Delete(Bytes.toBytes(row));
        table.delete(del);
        table.close();
    }



    /**
     * 根据行健删除多条数据
     * @param tableName  表名称
     * @param rows  要删除的多个行健
     * @throws Exception
     */
    public static void delMultiRows(String tableName, String[] rows)
            throws Exception {
        HTable table = new HTable(conf, tableName);
        List<Delete> delList = new ArrayList<Delete>();
        for (String row : rows) {
            Delete del = new Delete(Bytes.toBytes(row));
            delList.add(del);
        }
        table.delete(delList);
        table.close();
    }

    /**
     * 获取所有数据,全表扫描
     * @param tableName  表名称
     * @throws Exception
     */
    public static ResultScanner getAllRows(String tableName) throws Exception {
        HTable table = new HTable(conf, tableName);
        Scan scan = new Scan();
        ResultScanner results = null;

        try{
            results = table.getScanner(scan);
        }catch (IOException io){
            LOG.error(io.getMessage());
        }

        return  results;
    }


    /**
     *
     * @param tableName  表名称
     * @param columnFamily  列族
     * @throws Exception
     */
    public static ResultScanner getAllByColumnFamily(String tableName,String columnFamily) throws Exception {
        HTable table = new HTable(conf, tableName);
        Scan scan = new Scan();
        scan.addFamily(Bytes.toBytes(columnFamily));
        scan.setCaching(500);
        scan.setCacheBlocks(false);
        ResultScanner results = null;
        try{
            results=table.getScanner(scan);
        }catch (IOException io){
            LOG.error(io.getMessage());
        }

        return results;
    }

    /**
     *根据rowKey的范围查看数据
     * @param tableName  表名称
     * @param start_rowkey  开始行健
     * @param stop_rowkey   结束行健
     * @throws IOException
     */
    public static ResultScanner getByRowkeyRange(String tableName, String start_rowkey,
                                      String stop_rowkey) throws IOException {
        HTable table = new HTable(conf, Bytes.toBytes(tableName));
        Scan scan = new Scan();
        scan.setStartRow(Bytes.toBytes(start_rowkey));
        scan.setStopRow(Bytes.toBytes(stop_rowkey));
        ResultScanner result = null;
        try {
            result = table.getScanner(scan);
        } catch (IOException io){
            LOG.error(io.getMessage());
        }

        return  result;
    }

    /**
     *  删除表指定的列
     * @param tableName
     * @param rowKey
     * @param falilyName
     * @param columnName
     * @throws IOException
     */
    public static void delColumn(String tableName, String rowKey,
                                    String falilyName, String columnName) throws IOException {
        HTable table = new HTable(conf, Bytes.toBytes(tableName));
        Delete deleteColumn = new Delete(Bytes.toBytes(rowKey));
        deleteColumn.deleteColumns(Bytes.toBytes(falilyName),
                Bytes.toBytes(columnName));
        table.delete(deleteColumn);
        table.close();
    }


    public static void main(String[] args) throws Exception{
        ResultScanner userInfo = ConHbaseServices.getAllRows("userInfo");
        for (Result result : userInfo) {
            for (KeyValue rowKV : result.raw()) {
                System.out.print("行名:" + new String(rowKV.getRow()) + " ");
                System.out.print("时间戳:" + rowKV.getTimestamp() + " ");
                System.out.print("列族名:" + new String(rowKV.getFamily()) + " ");
                System.out.print("列名:" + new String(rowKV.getQualifier()) + " ");
                System.out.println("值:" + new String(rowKV.getValue()));
            }
        }
    }
}
