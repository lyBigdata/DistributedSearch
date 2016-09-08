package es;

import org.apache.log4j.Logger;

/**
 * <pre>
 * User: liuyu
 * Date: 2016/9/6
 * Time: 10:25
 * </pre>
 *
 * @author liuyu
 */

/**
 * ELK测试使用,实时产生日志数据,有logstash收集,过滤后,发送到es创建索引
 */
public class Log4jTest {

    private static final Logger LOGGER = Logger.getLogger(Log4jTest.class);
    public static void main(String[] args) throws Exception {
        for (int i = 0; i < 100; i++) {
            LOGGER.error("Info log [" + i + "].");
            Thread.sleep(500);
        }
    }
}
