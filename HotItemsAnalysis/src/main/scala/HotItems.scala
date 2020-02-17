import java.sql.Timestamp
import java.util.Properties

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.api.java.tuple.{Tuple, Tuple1}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.flink.util.Collector

import scala.collection.mutable.ListBuffer



//输入样例类
case class UserBehavior(userId:Long,itemId:Long,categoryId:Int,behavior:String,timestamp:Long)

case class ItemViewCount(itemId:Long,windowEnd:Long,count:Long)

object HotItems {
  def main(args: Array[String]): Unit = {
    //１. 创建执行环境
    val env=StreamExecutionEnvironment.getExecutionEnvironment
    //指定Time类型为EventTime
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    //并发度(slot)
    env.setParallelism(1)

    //2. 读取数据
      //2.1　kafka数据源
    val properties=new Properties()
    properties.setProperty("bootstrap.servers","localhost:9092") //访问地址及端口号
    properties.setProperty("group.id","consumer-group")
    properties.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    properties.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    properties.setProperty("auto.offset.reset", "latest") //自动提交重置偏移量

    val dataStream=env.addSource(new FlinkKafkaConsumer[String]("hotitems",new SimpleStringSchema(),properties))
      //2.2 文件数据源
    //val dataStream=env.readTextFile("")
      .map(line=>{
        val linearray=line.split(",")
        UserBehavior(linearray(0).trim.toLong,linearray(1).trim.toLong,linearray(2).trim.toInt,linearray(3).trim,linearray(4).trim.toLong)
      })
      //抽取时间戳和生成watermark(由于数据源的每条数据的时间戳是单调递增的，所有将每条数据的业务时间当做watermark，若是乱序数据，需要使用BoundedOutOfOrdernessTimestampExtractor)
      .assignAscendingTimestamps(_.timestamp*1000L) //时间戳转为ms单位

    // 3. 转换处理数据
    val processedStream=dataStream
      .filter(_.behavior=="pv") //过滤点击事件
      .keyBy(_.itemId) //对商品分组，分成多个并行流
      .timeWindow( Time.minutes(60),Time.minutes(5) )   //5分钟一个窗口(若后面没有延迟，则会５分钟输出一个窗口)
      .aggregate(new CountAgg(),new WindowResult())
        .keyBy(_.windowEnd) //按窗口结束时间分组
        .process(new TopNHotItems(3))

    //4. sink,控制台输出
    processedStream.print()

    env.execute("Hot Items Job")
  }

}

//自定义预聚合函数(累加器)，适合简单聚合
class CountAgg() extends AggregateFunction[UserBehavior,Long,Long]{
  override def add(in: UserBehavior, acc: Long): Long = acc+1

  //累加器初始值为０
  override def createAccumulator(): Long = 0L

  override def getResult(acc: Long): Long = acc

  override def merge(acc: Long, acc1: Long): Long = acc+acc1
}



//自定义预聚合函数计算平均数
/**
 * 输入类型:UserBehavior
 * 中间聚合状态类型:(Long,Int) ->Ｌong:时间戳和，Ｉnt:个数
 * 输出类型:Double
 */
class AverageAgg() extends AggregateFunction[UserBehavior,(Long,Int),Double]{
  //当前输入值：in  ,中间状态:acc
  override def add(in: UserBehavior, acc: (Long, Int)): (Long, Int) = (acc._1+in.timestamp,acc._2+1)

  override def createAccumulator(): (Long, Int) = (0L,0)

  override def getResult(acc: (Long, Int)): Double = acc._1/acc._2
  //合并两个累加器，仍是中间状态的改变
  override def merge(acc: (Long, Int), acc1: (Long, Int)): (Long, Int) = (acc._1+acc1._1,acc._2+acc1._2)
}



//自定义窗口函数，输出ＩtemViewCount(商品ＩＤ、)
/**
 * 输入类型:预聚合函数的输出 Long
 * 输出类型: ItemViewCount
 * ｋey类型: Tuple类型
 * Window类型：TimeWindow
 */
class WindowResult() extends WindowFunction[Long,ItemViewCount,Long,TimeWindow]{
  override def apply(key: Long, window: TimeWindow, input: Iterable[Long], out: Collector[ItemViewCount]): Unit = {

    out.collect(ItemViewCount(key,window.getEnd,input.iterator.next()))
  }
}



//自定义的处理函数,
/**
 * ｋey类型:Long
 * 输入类型:ItemViewCount
 * 输出类型:String
 * @param topSize
 */
class TopNHotItems(topSize: Int) extends KeyedProcessFunction[Long,ItemViewCount,String]{
  private var itemState:ListState[ItemViewCount]= _ //默认空值


  override def open(parameters: Configuration): Unit = {
    itemState=getRuntimeContext.getListState(new ListStateDescriptor[ItemViewCount]("item-state",classOf[ItemViewCount]))
  }

  override def processElement(value: ItemViewCount, context: KeyedProcessFunction[Long, ItemViewCount, String]#Context, collector: Collector[String]): Unit = {
    //将每条到来的数据存入状态列表
    itemState.add(value)
    //注册一个定时器
    context.timerService().registerEventTimeTimer(value.windowEnd+1)  //延迟１s触发
  }

  //定时器触发时，对所有数据排序，并输出结果
  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, ItemViewCount, String]#OnTimerContext, out: Collector[String]): Unit = {
    //将所有state中数据取出放入Lｉst Buffer中
    val allItems:ListBuffer[ItemViewCount]=new ListBuffer()

    //遍历引入转换包
    import scala.collection.JavaConversions._
    for(item<-itemState.get()){
      allItems+=item
    }

    //按照点击量(ｃount)降序，并取前Ｎ
    val sortedItems=allItems.sortBy(_.count)(Ordering.Long.reverse).take(topSize)

    //清空状态
    itemState.clear()

    //将排名记过格式化输出
    val result:StringBuilder=new StringBuilder()
    result.append("时间: ").append(new Timestamp(timestamp-1)) //窗口关闭时间timestamp为定时器触发时间
      .append("\n")

    //输出每个商品信息
    for (i<-0 to sortedItems.length-1){
      val currentItem=sortedItems(i)
      result.append("No").append(i+1).append(":")
        .append(" 商品ＩD=").append(currentItem.itemId)
        .append( "浏览量＝").append(currentItem.count)
        .append("\n")
    }
    result.append("=============================")
    //控制输出频率
    Thread.sleep(1000)
    out.collect(result.toString())


  }
}