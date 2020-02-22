package org.bigdata.storm.wordcount;

import org.apache.commons.lang3.StringUtils;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;

import java.util.*;

/**
 * @description: Storm入门程序-wordCount
 * @author: fanyeuxiang
 * @createDate: 2020-02-21
 */
public class DataSourceSpout extends BaseRichSpout {

    private List<String> list = Arrays.asList("Spark", "Hadoop", "HBase", "Storm", "Flink", "Hive");

    private SpoutOutputCollector spoutOutputCollector;

    /**
     * @param conf Spout 的配置
     * @param topologyContext 应用上下文，可以通过其获取任务 ID 和组件 ID，输入和输出信息等
     * @param spoutOutputCollector 用来发送 spout 中的 tuples，它是线程安全的，建议保存为此 spout 对象的实例变量
     */
    @Override
    public void open(Map conf, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        this.spoutOutputCollector = spoutOutputCollector;
    }

    /**
     * 此方法内部发送 tuples
     */
    @Override
    public void nextTuple() {
        // 模拟生产数据
        String lineData = productData();
        System.out.println("productData: "+ lineData);
        spoutOutputCollector.emit(new Values(lineData));
        Utils.sleep(1000);
    }

    /**
     * 声明发送的 tuples 的名称
     *
     * @param outputFieldsDeclarer
     */
    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("line"));
    }

    /**
     * 模拟数据
     */
    private String productData() {
        Collections.shuffle(list);
        Random random = new Random();
        int endIndex = random.nextInt(list.size()) % (list.size()) + 1;
        return StringUtils.join(list.toArray(), "\t", 0, endIndex);
    }
}
