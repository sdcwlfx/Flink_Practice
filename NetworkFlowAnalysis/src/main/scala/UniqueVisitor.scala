import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.function.AllWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector


//输出样例类
case class UvCount(windowEnd:Long,count:Long)
//输入样例类
case class UserBehavior(userId:Long,itemId:Long,categoryId:Int,behavior:String,timestamp:Long)


/**
 * UV统计
 */
object UniqueVisitor {
  def main(args: Array[String]): Unit = {
    //１. 执行环境
    val env=StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)

    //获取资源相对路径
    val resourcesPath=getClass.getResource("/UserBehaviorTest.csv")

    val dataStream=env.readTextFile(resourcesPath.getPath)
      .map(line=>{
        val linearray=line.split(",")
        UserBehavior(linearray(0).trim.toLong,linearray(1).trim.toLong,linearray(2).trim.toInt,linearray(3).trim,linearray(4).trim.toLong)
      })
      //提取时间戳并制定watermark
      .assignAscendingTimestamps(_.timestamp*1000)
      .filter(_.behavior=="pv")
      .timeWindowAll(Time.seconds(60*60))  //未keyby使用windowAll窗口,滚动窗口
      .apply(new UvCountByWindow())
      .print()
    env.execute("Unique Visitor Job")

  }
}

/**
 * 对窗口内的去重后的userId计数
 * 输入类型:
 * 输出类型:
 * 窗口类型:
 */
class UvCountByWindow() extends AllWindowFunction[UserBehavior,UvCount,TimeWindow]{
  override def apply(window: TimeWindow, input: Iterable[UserBehavior], out: Collector[UvCount]): Unit = {
    //定义set，保存所有数据的userId并去重
    var idSet=Set[Long]() //都必须全部放进内存中，但数据量很大时，无法使用，可以使用位图来存储数据
    //当前窗口所有数据userId收集到set中，并输出set的大小
    for (userBehavior<-input){
      idSet+=userBehavior.userId
    }
    out.collect(UvCount(window.getEnd,idSet.size))
  }
}
