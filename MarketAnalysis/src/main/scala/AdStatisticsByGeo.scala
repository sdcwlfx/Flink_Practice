import java.sql.Timestamp

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector


//输入广告点击事件样例类
case class AdClickEvent(userId:Long,adId:Long,province:String,city:String,timestamp:Long)
//按照省份统计点击量的输出结果样例类
case class CountByProvince(windowEnd:String,province:String,count:Long)

/**
 * 页面广告按照省份划分的一个小时内点击量的统计，
 * 并5s更新一次，从而了解用户偏好进而进行推荐等操作
 */

object AdStatisticsByGeo {
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

    //３. 根据省份做分组,开窗口聚合
    val adCountStream=adEventTime
      .keyBy(_.province)
      .timeWindow(Time.hours(1),Time.seconds(5))//滑动窗口，每５s统计一次
      .aggregate(new AdCountAgg(),new AdCountResult())//AdCountAgg()对窗口中数据进行增量预聚合，产生结果作为AdCountResult()的输入，作为窗口处理函数
      .print()

    env.execute("ad statistics job")
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
