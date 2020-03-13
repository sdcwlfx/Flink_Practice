
import java.sql.Timestamp
import java.text.SimpleDateFormat

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import scala.collection.mutable.ListBuffer


//输入样例类
case class ApacheLogEvent(ip:String,userId:String,eventTime:Long,method:String,url:String)

//窗口聚合结果样例
case class UrlViewCount(url:String,windowEnd:Long,count:Long)

/**
 * 热门页面ＴopN
 */
object NetworkFlow {
  def main(args: Array[String]): Unit = {
    val env=StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val dataStream=env.readTextFile("/home/hadoop/IdeaProjects/UserBehaviorAnalysis/NetworkFlowAnalysis/src/main/resources/apache.log")
      .map(data=>{
        val dataArray=data.split(" ")
        //定义时间转换
        val simpleDateFormat=new SimpleDateFormat("dd/MM/yyyy:HH:mm:ss")
        val timestamp=simpleDateFormat.parse(dataArray(3).trim).getTime //得到时间戳(ms)
        ApacheLogEvent(dataArray(0).trim,dataArray(1).trim,timestamp,dataArray(5).trim,dataArray(6).trim)
      })
      //处理乱序数据：提取时间并生成watermark，需要给出延迟时间处理迟到数据
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[ApacheLogEvent](Time.seconds(1)) {
        override def extractTimestamp(t: ApacheLogEvent): Long = t.eventTime
      })
      .keyBy(_.url)
      .timeWindow(Time.minutes(10),Time.seconds(5))
      //允许延迟６０s
      .allowedLateness(Time.seconds(60))
      .aggregate(new CountAgg(),new WindowResult())  //窗口聚合
      .keyBy(_.windowEnd)
      .process(new TopNHotUrls(5))

    dataStream.print()

    env.execute("network flow job")

  }

}

//自定义预聚合函数
/**
 * 输入类型:ApacheLogEvent
 * 中间状态类型:Long
 * 输出类型:Long
 */
class CountAgg() extends AggregateFunction[ApacheLogEvent,Long,Long]{
  override def add(in: ApacheLogEvent, acc: Long): Long = acc+1

  override def createAccumulator(): Long = 0L

  override def getResult(acc: Long): Long = acc

  override def merge(acc: Long, acc1: Long): Long = acc+acc1
}

//自定义窗口内处理函数
/**
 * 输入类型:Long
 * 输出类型:UrlViewCount
 * key类型:String
 * 窗口类型：TimeWindow
 */
class WindowResult() extends WindowFunction[Long,UrlViewCount,String,TimeWindow]{
  override def apply(key: String, window: TimeWindow, input: Iterable[Long], out: Collector[UrlViewCount]): Unit = {
    out.collect(UrlViewCount(key,window.getEnd,input.iterator.next()))
  }
}




//ＴｏｐN排序统计函数
class TopNHotUrls(topSize:Int) extends KeyedProcessFunction[Long,UrlViewCount,String]{
  lazy val urlState: ListState[UrlViewCount] = getRuntimeContext.getListState(new ListStateDescriptor[UrlViewCount]("url-state",classOf[UrlViewCount]))

  override def processElement(value: UrlViewCount, context: KeyedProcessFunction[Long, UrlViewCount, String]#Context, collector: Collector[String]): Unit = {
    //将每个元素存入状态列表中
    urlState.add(value)
    context.timerService().registerEventTimeTimer(value.windowEnd+1) //定时器
  }

  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, UrlViewCount, String]#OnTimerContext, out: Collector[String]): Unit = {
    //从状态获取数据
    val allUrlViews:ListBuffer[UrlViewCount]=new ListBuffer[UrlViewCount]()
    val iter=urlState.get().iterator()  //获取迭代器
    while (iter.hasNext){
      allUrlViews+=iter.next()
    }
    urlState.clear() //统计后清空状态
    val sortedUrlViews=allUrlViews.sortWith(_.count>_.count).take(topSize) //按照count降序

    //格式化输出
    val result:StringBuilder=new StringBuilder()
    result.append("时间：　").append(new Timestamp(timestamp-1)).append("\n")
    for(i<-sortedUrlViews.indices){
      val currentUrlView=sortedUrlViews(i)
      result.append("NO").append(i+1).append(":")
        .append(" URL=").append(currentUrlView.url)
        .append(" 访问量").append(currentUrlView.count).append("\n")
    }
    result.append("===========================")
    Thread.sleep(1000)
    out.collect(result.toString())
  }
}