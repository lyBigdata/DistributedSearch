package es;

/**
 * <pre>
 * User: liuyu
 * Date: 2016/9/8
 * Time: 18:07
 * </pre>
 *
 * @author liuyu
 */
public class DataImportHBaseAndIndex {

    public static final String FILE_PATH= "D:/bigdata/es_hbase/datasrc/article.txt" ;
    public static void main(String[] args) throws java.lang.Exception {
        // 读取数据源
        InputStream in = new FileInputStream(new File(FILE_PATH ));
        BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(in ,"UTF-8" ));
        String line = null;
        List<Article> articleList = new ArrayList<Article>();
        Article article = null;
        while ((line = bufferedReader .readLine()) != null) {
            String[] split = StringUtils.split(line, "\t");
            article = new Article();
            article.setId( new Integer(split [0]));
            article.setTitle( split[1]);
            article.setAuthor( split[2]);
            article.setDescribe( split[3]);
            article.setContent( split[3]);
            articleList.add(article );
        }

        for (Article a : articleList ) {
            // HBase插入数据
            HBaseUtils hBaseUtils = new HBaseUtils();
            hBaseUtils.put(hBaseUtils .TABLE_NAME , String.valueOf(a.getId()), hBaseUtils.COLUMNFAMILY_1 ,
                    hBaseUtils.COLUMNFAMILY_1_TITLE , a .getTitle());
            hBaseUtils.put(hBaseUtils .TABLE_NAME , String.valueOf(a.getId()), hBaseUtils.COLUMNFAMILY_1 ,
                    hBaseUtils.COLUMNFAMILY_1_AUTHOR , a.getAuthor());
            hBaseUtils.put(hBaseUtils .TABLE_NAME , String.valueOf(a.getId()), hBaseUtils.COLUMNFAMILY_1 ,
                    hBaseUtils.COLUMNFAMILY_1_DESCRIBE , a.getDescribe());
            hBaseUtils.put(hBaseUtils .TABLE_NAME , String.valueOf(a.getId()), hBaseUtils.COLUMNFAMILY_1 ,
                    hBaseUtils.COLUMNFAMILY_1_CONTENT , a.getContent());

            // ElasticSearch 插入数据
            EsUtil. addIndex(EsUtil.DEFAULT_INDEX, EsUtil.DEFAULT_TYPE , a );
        }
    }
}