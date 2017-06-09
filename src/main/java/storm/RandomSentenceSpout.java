package storm;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class RandomSentenceSpout extends BaseRichSpout {
  private static final Logger LOG = LoggerFactory.getLogger(RandomSentenceSpout.class);
  SpoutOutputCollector _collector;
  private static int co=0;
  private long msgId = 0;

  @Override
  public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
    _collector = collector;
  }

  @Override
  public void nextTuple() {
    if(co<1000) {
      Utils.sleep(200);
      String sentence = "cow over moon";
      co++;
      // _collector.emit(new Values(sentence),++msgId);
      _collector.emit(new Values(sentence));
    }
  }

  @Override
  public void ack(Object id) {
      LOG.debug("Got ACK for msgId : " + msgId);
  }

  @Override
  public void fail(Object id) {
      LOG.debug("Got FAIL for msgId : " + msgId);
  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer) {
    declarer.declare(new Fields("sentence"));
  }

}
