import java.sql.Timestamp
import java.util.UUID
import java.util.concurrent.TimeUnit

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.source.{RichSourceFunction, SourceFunction}
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import scala.util.Random

//输入样例类
case class MarketingUserBehavior(UserId:String,behavior:String,channel:String,timestamp:Long)
//输出结果样例类
case class MarketingViewCount(windowStart:String,windowEnd:String,channel:String,behavior:String,count:Long)


/**
 * Ａpp市场推广－分渠道统计
 */
object AppMarketingByChannel {
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
        ((data.channel,data.behavior),1L)
      })//将数据封装成二元组
      .keyBy(_._1)//以渠道和分类为key分组
      .timeWindow(Time.hours(1),Time.seconds(10))//１０秒滑动窗口
      .process(new MarketingCountByChannel())//对窗口内数据执行动作

    datastream.print()
    env.execute("app marketing by channel job")
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

//自定义窗口处理函数
/**
 * 输入类型:((String,String),Long)
 * 输出类型:MarketingViewCount
 * key类型:(String,String)
 * 窗口类型:TimeWindow
 */
class MarketingCountByChannel() extends ProcessWindowFunction[((String,String),Long),MarketingViewCount,(String,String),TimeWindow]{

  //elements包含分组后的窗口内所有数据
  override def process(key: (String, String), context: Context, elements: Iterable[((String, String), Long)], out: Collector[MarketingViewCount]): Unit = {
    val startTs=new Timestamp(context.window.getStart).toString
    val endTs=new Timestamp(context.window.getEnd).toString
    val channel=key._1
    val behavior=key._2
    val count=elements.size

    out.collect(MarketingViewCount(startTs,endTs,channel,behavior,count))
  }
}

