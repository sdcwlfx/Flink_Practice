import java.sql.Timestamp

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala.{OutputTag, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector


//输入广告点击事件样例类
case class AdClickEvent(userId:Long,adId:Long,province:String,city:String,timestamp:Long)
//按照省份统计点击量的输出结果样例类
case class CountByProvince(windowEnd:String,province:String,count:Long)
//输出的黑名单报警信息【用户id,广告id】
case class BlackListWarning(userId:Long,adId:Long,message:String)



/**
 * 带黑名单的页面点击量统计，记录用户点击次数，
 * 将连续刷单的用户加入黑名单(状态编程),
 * 并且每天将黑名单清空(定时器)
 * 侧输出流输出报警信息
 */
object AdStatisticsByGeoWithBlack {

  //定义侧输出流（）的标签。输出过滤的报警信息
  val blackListOutputTag:OutputTag[BlackListWarning]=new OutputTag[BlackListWarning]("blackList")

  def main(args: Array[String]): Unit = {
    //1. 执行环境
    val env=StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)//采用的时间语义ＥventTime

    //２. 获取数据源
    val resource=getClass.getResource("/AdClickLog.csv")//获取数据源相对路径
    val adEventTime=env.readTextFile(resource.getPath)
      .map(data=>{
        val dataArray=data.split(",")
        AdClickEvent(dataArray(0).trim.toLong,dataArray(1).trim.toLong,dataArray(2).trim,dataArray(3).trim,dataArray(4).trim.toLong)
      })
      .assignAscendingTimestamps(_.timestamp*1000L)//提取时间戳并指定watermark,数据源时间戳为升序

    //自定义process函数，过滤刷单行为
    val filterBlackListStream=adEventTime
      .keyBy(data=>(data.userId,data.adId))//根据用户id、广告id进行分组
      .process(new FilterBlackListUser(100))//传入设置的点击量阈值(一天同一用户对同一广告点击大于１００次后，过滤掉从侧输出流输出)



    //３. 根据省份做分组,开窗口聚合
    val adCountStream=filterBlackListStream
      .keyBy(_.province)
      .timeWindow(Time.hours(1),Time.seconds(5))//滑动窗口，每５s统计一次
      .aggregate(new AdCountAgg(),new AdCountResult())//AdCountAgg()对窗口中数据进行增量预聚合，产生结果作为AdCountResult()的输入，作为窗口处理函数
      .print()

    //通过标签获取侧输出流内容并输出
    filterBlackListStream.getSideOutput(blackListOutputTag).print("blackList")

    env.execute("ad statistics with black job")

  }

  //自定义窗口处理函数
  class FilterBlackListUser(maxCount: Int) extends KeyedProcessFunction[(Long,Long),AdClickEvent,AdClickEvent]{
    //定义状态,保存当前用户对当前广告的点击量,由于前面已经keyby过了，数据的用户id和广告id均一样,所以只需要一个值保存点击量即可
    lazy val countState:ValueState[Long]=getRuntimeContext.getState(new ValueStateDescriptor[Long]("count-state",classOf[Long]))
    //保存是否发送过黑名单的状态
    lazy val isSentBlackList:ValueState[Boolean]=getRuntimeContext.getState(new ValueStateDescriptor[Boolean]("issent-black",classOf[Boolean]))
    //保存定时器触发的时间戳
    lazy val resetTimer:ValueState[Long]=getRuntimeContext.getState(new ValueStateDescriptor[Long]("reset-timer",classOf[Long]))

    override def processElement(value: AdClickEvent, context: KeyedProcessFunction[(Long, Long), AdClickEvent, AdClickEvent]#Context, collector: Collector[AdClickEvent]): Unit = {
      //取出状态
      val curCount=countState.value()//取出当前用户对当前广告的点击量

      //如果是第一次点击，注册定时器，明天当前用户对当前商品的点击量清零
      if(curCount==0){
        val ts=(context.timerService().currentProcessingTime()/(1000*60*60*24)+1)*(1000*60*60*24)//明天00:00的时间戳
        resetTimer.update(ts)//保存定时器触发时间
        context.timerService().registerProcessingTimeTimer(ts)//注册系统时间定时器

      }

      //判断计数是否达到上限，如果达到则加入黑名单
      if(curCount>=maxCount){
        //判断是否发送过黑名单，只发送一次
        if(!isSentBlackList.value()){
          isSentBlackList.update(true)
          //输出到侧输出流
          context.output(blackListOutputTag,BlackListWarning(value.userId,value.adId,"Click over "+maxCount+" times"))
        }

      }else{
        //计数加１，输出数据到主流
        countState.update(curCount+1)
        collector.collect(value)//输出到主流
      }

    }

    //定时器触发，清空状态
    override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[(Long, Long), AdClickEvent, AdClickEvent]#OnTimerContext, out: Collector[AdClickEvent]): Unit = {
      //达到定时器时间
      if(timestamp==resetTimer.value()){
        isSentBlackList.clear()
        countState.clear()
        resetTimer.clear()
      }
    }
  }

}



/**
 * 预聚合函数-累加
 * 输入类型：AdClickEvent
 * 中间状态类型：Long
 * 输出类型：Ｌong ->也是AdCountResult()的输入类型
 */
class AdCountAgg() extends AggregateFunction[AdClickEvent,Long,Long]{

  //每来一条数据累加器就+1
  override def add(in: AdClickEvent, acc: Long): Long =acc+1

  //初始化累加器
  override def createAccumulator(): Long = 0L

  override def getResult(acc: Long): Long = acc

  override def merge(acc: Long, acc1: Long): Long = acc+acc1

}


/**
 * 自定义窗口处理函数->窗口关闭时触发
 * 输入类型:Long
 * 输出类型:CountByProvince
 * key类型:String->即province
 * 窗口类型:TimeWindow
 */
class AdCountResult() extends WindowFunction[Long,CountByProvince,String,TimeWindow]{

  //对窗口内每个元素执行动作
  override def apply(key: String, window: TimeWindow, input: Iterable[Long], out: Collector[CountByProvince]): Unit = {
    val windowEnd=new Timestamp(window.getEnd).toString//窗口结束时间 年/月/日
    val count=input.iterator.next()//由于已经采用预聚合，故窗口中数据记录该时间窗口中的数据规模
    out.collect(CountByProvince(windowEnd,key,count))
  }
}

