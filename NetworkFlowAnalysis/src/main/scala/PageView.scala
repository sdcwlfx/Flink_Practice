
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

//输入样例类
case class UserBehavior(userId:Long,itemId:Long,categoryId:Int,behavior:String,timestamp:Long)

/**
 * 网络流量点击量(PV),利用前端埋点日志，设置滚动窗口
 */
object PageView {
  def main(args: Array[String]): Unit = {
    //１．执行环境
    val env=StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    //利用相对路径定义数据源
    val resource=getClass.getResource("/UserBehaviorTest.csv")
    val dataStream=env.readTextFile(resource.getPath)
      .map(data=>{
        val dataArray=data.split(",")
        UserBehavior(dataArray(0).trim.toLong,dataArray(1).trim.toLong,dataArray(2).trim.toInt,dataArray(3).trim,dataArray(4).trim.toLong)
      })
      .assignAscendingTimestamps(_.timestamp*1000L)
      .filter(_.behavior=="pv") //过滤点击操作
      .map(data=>("pv",1)) //每条数据转为("pv",1)元组
      .keyBy(_._1)//根据字段"pv"进行分组(不起作用，所有的数据都在一个组内)
      .timeWindow(Time.hours(1)) //滚动窗口(统计每小时内页面点击数目)，直接对窗口内"pv"累加，窗口长度＝1h
      .sum(1)

    dataStream.print("pv count")
    env.execute("Page View Job")
  }


}
