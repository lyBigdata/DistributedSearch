package es;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.codehaus.jackson.map.ObjectMapper;
import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.count.CountRequestBuilder;
import org.elasticsearch.action.count.CountResponse;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetRequestBuilder;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.action.update.UpdateRequestBuilder;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.collect.ImmutableList;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.text.Text;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.query.FilterBuilders;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.bucket.terms.Terms.Bucket;
import org.elasticsearch.search.aggregations.metrics.sum.Sum;
import org.elasticsearch.search.highlight.HighlightField;
import org.elasticsearch.search.sort.SortOrder;
import org.junit.Before;
import org.junit.Test;
/**
 * <pre>
 * User: liuyu
 * Date: 2016/9/8
 * Time: 18:41
 * </pre>
 *
 * @author liuyu
 */
public class EsUtils {
    // 客户端连接集群的示例对象
    private TransportClient client;
    //索引库
    private final String index="jf";
    //类型
    private final String type="ware_info";

    /**
     * 连接es
     */
    @Before
    public void init() {
        Properties properties = new Properties();
        properties.put("cluster.name", "elasticsearch");// 默认是指定集群名称，默认情况下集群名称elasticsearch
        properties.put("client.transport.sniff", "true");// 自动嗅探整个集群的状态,es会自动把集群中其它机器的ip地址加到客户端中

        Settings settings = ImmutableSettings.settingsBuilder().put(properties).build();
        TransportAddress transportAddress = new InetSocketTransportAddress("192.168.2.20", 9300);// 设置集群IP和端口，多个通过逗号分割

        client = new TransportClient(settings).addTransportAddress(transportAddress);
    }

    /**
     * 连接es
     *
     * @throws Exception
     */
    @Test
    public void test1() throws Exception {

        GetResponse response = client.prepareGet("jf", "ware_info", "200").execute().actionGet();
        String sourceAsString = response.getSourceAsString();
        System.out.println(sourceAsString);
    }

    /**
     * 查询索引存储所有节点
     *
     * @throws Exception
     */
    @Test
    public void test2() throws Exception {

        ImmutableList<DiscoveryNode> connectedNodes = client.connectedNodes();
        for (DiscoveryNode discoveryNode : connectedNodes) {
            System.out.println(discoveryNode.getHostAddress());
        }
    }
    /**
     * index-json
     * @throws Exception
     */
    @Test
    public void testIndexJson() throws Exception {
        String source = "{\"wareCode\":\"4001\",\"wareName\":\"90huafei\",\"kind\":\"huafei\"}";

        IndexRequestBuilder builder = client.prepareIndex(index, type, "401");
        builder.setSource(source);
        builder.execute();
    }

    /**
     * index-map
     * @throws Exception
     */
    @Test
    public void testIndexMap() throws Exception {
        IndexRequest request = new IndexRequest(index,type,"400");
        Map<String,Object> source = new HashMap<String,Object>();
        source.put("id", 400);
        source.put("wareCode", "4000");
        source.put("wareName", "80huafei");
        source.put("kind", "huafei");
        request.source(source);
        client.index(request);
    }

    /**
     * index-bean
     * @throws Exception
     */
    @Test
    public void testIndexBean() throws Exception {
        WareInfoBean bean = new WareInfoBean();
        bean.setId("402");
        bean.setWareCode("4002");
        bean.setWareName("90huafei");
        bean.setKind("huafei");

        IndexRequestBuilder builder = client.prepareIndex(index, type,bean.getId());
        builder.setSource(new ObjectMapper().writeValueAsString(bean));
        builder.execute();
    }

    /**
     * index-es helper
     * @throws Exception
     */
    @Test
    public void testIndexEsHelper() throws Exception {
        XContentBuilder sourceBuilder = XContentFactory.jsonBuilder();
        sourceBuilder.startObject();
        sourceBuilder.field("id", "501");
        sourceBuilder.field("wareCode", "5001");
        sourceBuilder.field("wareName", "100huafei");
        sourceBuilder.field("kind", "huafei");

        sourceBuilder.endObject();
        IndexRequestBuilder builder = client.prepareIndex(index, type, "501");
        builder.setSource(sourceBuilder);
        builder.execute();
    }


    /**
     * update
     * @throws Exception
     */
    @Test
    public void testUpate() throws Exception{
        UpdateRequestBuilder builder = client.prepareUpdate(index, type, "501");
        Map<String,Object> source = new HashMap<String,Object>();
        source.put("kind","huafei-update");
        builder.setDoc(source);
        builder.execute();
    }

    /**
     * delete ById
     * @throws Exception
     */
    @Test
    public void testDeleteById() throws Exception{
        DeleteResponse response = client.prepareDelete(index, type, "501").execute().actionGet();
        System.out.println(response.getId());
    }

    /**
     * count
     * @throws Exception
     */
    @Test
    public void testTotal() throws Exception{
        CountRequestBuilder builder = client.prepareCount(index);
        CountResponse response = builder.execute().get();
        long count = response.getCount();
        System.out.println("count:"+count);
    }


    /**
     * bulk
     * @throws Exception
     */
    @Test
    public void testBulk() throws Exception{

        BulkRequest request = new BulkRequest();
        IndexRequest indexRequest = new IndexRequest(index,type,"600");
        Map<String,Object> source = new HashMap<String,Object>();
        source.put("id", "600");
        source.put("wareCode", "6000");
        source.put("wareName", "100huafei");
        source.put("kind", "huafei");
        request.add(indexRequest.source(source));

        ActionFuture<BulkResponse> bulk = client.bulk(request);

        BulkResponse response = bulk.get();
        if(response.hasFailures()){
            BulkItemResponse[] items = response.getItems();
            for (BulkItemResponse bulkItemResponse : items) {
                System.out.println(bulkItemResponse.getFailureMessage());
            }
        }
    }

    /**
     * get by id
     * @throws Exception
     */
    @Test
    public void testIndexGetById() throws Exception {
        GetRequestBuilder builder = client.prepareGet(index,type,"501");
        GetResponse response = builder.execute().get();
        Map<String, Object> source = response.getSource();
        for (Map.Entry<String, Object> entry : source.entrySet()) {
            System.out.println(entry.getKey() + ":" + entry.getValue());
        }
    }


    /**
     * 关键字查询
     * @throws Exception
     */
    @Test
    public void testGetByKeyWords() throws Exception {
        SearchRequestBuilder builder = client.prepareSearch(index);
        builder.setTypes(type);
        builder.setSearchType(SearchType.QUERY_THEN_FETCH);//搜索类型
        builder.setQuery(QueryBuilders.matchQuery("wareName","30liuliang"));// 设置查询条件
        builder.setPostFilter(FilterBuilders.rangeFilter("countValue").from(1000).to(5000));//设置过滤条件
        builder.addSort("countValue", SortOrder.ASC);//字段排序


        builder.addHighlightedField("wareName");//设置高亮字段
        builder.setHighlighterPreTags("<font red='colr'>");//设置高亮前缀
        builder.setHighlighterPostTags("</font>");//设置高亮后缀


        builder.setFrom(0);//pageNo 开始下标
        builder.setSize(2);//pageNum 共显示多少条

        SearchResponse response = builder.get();
        SearchHits hits = response.getHits();
        long total = hits.getTotalHits();//总数
        System.out.println("total:"+total);

        SearchHit[] hits2 = hits.getHits();
        for (SearchHit searchHit : hits2) {

            //获取高亮字段
            Map<String, HighlightField> highlightFields = searchHit.getHighlightFields();
            HighlightField highlightField = highlightFields.get("wareName");

            Map<String, Object> source = searchHit.getSource();
            for (Map.Entry<String, Object>  entry: source.entrySet()) {
                String key = entry.getKey();
                String value = "";
                if("wareName".equals(key)){
                    Text[] fragments = highlightField.getFragments();
                    for (Text text : fragments) {
                        value = text.toString();
                    }
                }else {
                    value = entry.getValue().toString();
                }
                System.out.println(key + "\t" + value);
            }

            String sourceAsString = searchHit.getSourceAsString();
            System.out.println(sourceAsString);
        }
    }

    /**
     * 分组统计 -等于与select kind ,count(1) from ware_info group by kind
     * @throws Exception
     */
    @Test
    public void testCount() throws Exception{

        SearchRequestBuilder builder = client.prepareSearch(index).setTypes(type);
        builder.addAggregation(AggregationBuilders.terms("count").field("kind"));
        SearchResponse reponse = builder.execute().get();

        Terms terms = reponse.getAggregations().get("count");
        List<Bucket> buckets = terms.getBuckets();
        for (Bucket bucket : buckets) {
            String r = bucket.getKey() + "->" + bucket.getDocCount();
            System.out.println(r);
        }
    }

    /**
     * 分组求和 -等于select kind ,sum(countValue) from ware_info group by kind
     * @throws Exception
     */
    @Test
    public void testSum() throws Exception{

        SearchRequestBuilder builder = client.prepareSearch(index).setTypes(type);
        builder.addAggregation(AggregationBuilders.terms("term").field("kind").subAggregation(AggregationBuilders.sum("sum").field("countValue")));
        SearchResponse reponse = builder.execute().get();

        Terms terms = reponse.getAggregations().get("term");
        List<Bucket> buckets = terms.getBuckets();
        for (Bucket bucket : buckets) {
            Sum sum = bucket.getAggregations().get("sum");
            String r = bucket.getKey() + "->" + sum.getValue();
            System.out.println(r);
        }
    }
}
