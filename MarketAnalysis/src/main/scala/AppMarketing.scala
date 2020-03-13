import java.sql.Timestamp
import java.util.UUID
import java.util.concurrent.TimeUnit

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.source.{RichSourceFunction, SourceFunction}
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import scala.util.Random

//输入样例类
case class MarketingUserBehavior(UserId:String,behavior:String,channel:String,timestamp:Long)
//输出结果样例类
case class MarketingViewCount(windowStart:String,windowEnd:String,channel:String,behavior:String,count:Long)

/**
 * Ａpp市场推广－不分渠道统计
 */
object AppMarketing {
  def main(args: Array[String]): Unit = {
    //１. 执行环境
    val env=StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    //读取数据源
    val datastream=env.addSource(new SimulatedEventSource())
      .assignAscendingTimestamps(_.timestamp)//生成watermark
      .filter(_.behavior!="UNINSTALL")//过滤掉卸载操作
      .map(data=>{
        ("dummyKey",1L)
      })//将数据封装,"dummyKey"只是用作key使用
      .keyBy(_._1)//以“dummy”为key分组，实际结果只有一组
      .timeWindow(Time.hours(1),Time.seconds(10))//１０秒滑动窗口
      .aggregate(new CountAgg(),new MarketingCountTotal())//对窗口内数据执行增量聚合动作,CountAgg()的输出会作为MarketingCountTotal()的输入

    datastream.print()
    env.execute("app marketing　job")

  }

}


//预聚合累加函数
/**
 * 输入类型:(String,Long)
 * 累加中间状态类型：Long
 * 输出类型:Long
 */
class CountAgg() extends AggregateFunction[(String,Long),Long,Long]{
  override def add(in: (String, Long), acc: Long): Long =acc+1

  override def createAccumulator(): Long = 0L

  override def getResult(acc: Long): Long =acc

  override def merge(acc: Long, acc1: Long): Long = acc+acc1

}

//窗口处理函数
/**
 * 输入类型:Long ->即预聚合函数的输出
 * 输出类型:MarketingViewCount
 * ｋey类型:String
 * 窗口类型：TimeWindow
 */
class MarketingCountTotal() extends WindowFunction[Long,MarketingViewCount,String,TimeWindow]{
  override def apply(key: String, window: TimeWindow, input: Iterable[Long], out: Collector[MarketingViewCount]): Unit = {
    val startTs=new Timestamp(window.getStart).toString
    val endTs=new Timestamp(window.getEnd).toString
    val count=input.iterator.next()//取得之前预聚合得到的结果

    out.collect(MarketingViewCount(startTs,endTs,"app marketing","total",count))
  }
}




//自定义数据源
class SimulatedEventSource() extends RichSourceFunction[MarketingUserBehavior]{
  //定义是否运行的标志位,若运行则不断产生数据
  var running=true//变量
  //定义用户行为集合
  val behaviorTypes:Seq[String]=Seq("CLICK","DOWNLOAD","INSTALL","UNSTALL")
  //定义渠道
  val channelSets:Seq[String]=Seq("wechat","weibo","appstore","xiaomistore")
  //定义一个随机数发生器
  val rand:Random=new Random()

  override def cancel(): Unit = {
    running=false
  }

  override def run(sourceContext: SourceFunction.SourceContext[MarketingUserBehavior]): Unit = {
    //定义一个生成数据的上限
    val maxElements=Long.MaxValue//常两
    var count=0L //变量－>数据总数

    //随机生成数据(某用户在ts时间从)
    while (running && count<maxElements){
      val userId=UUID.randomUUID().toString
      val behavior=behaviorTypes(rand.nextInt(behaviorTypes.size))
      val channel=channelSets(rand.nextInt(channelSets.size))
      val ts=System.currentTimeMillis()//ｍｓ

      sourceContext.collect(MarketingUserBehavior(userId,behavior,channel,ts))
      count+=1
      TimeUnit.MILLISECONDS.sleep(10L)//１０ms休息一次

    }
  }

}
