import java.util

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.api.scala._
import org.apache.flink.cep.PatternSelectFunction
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.cep.scala.CEP



//输入登录事件数据样例类 [用户id,登录ip,返回类型(成功/失败),登录时间戳]
//case class LoginEvent(userId:Long,ip:String,eventType:String,eventTime:Long)
case class LoginEvent(userId:Long,ip:String,eventType:String,eventTime:Long)
//输出报警信息样例类
case class Warning(userId:Long,firstFailTime:Long,lastFailTime:Long,warning:String)



object LoginFailCep {
  def main(args: Array[String]): Unit = {
    //1. 执行环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime) //采用的时间语义ＥventTime

    //读取事件数据
    val resource = getClass.getResource("/LoginLog.csv")
    val loginEventStream = env.readTextFile(resource.getPath)
      .map(data => {
        val dataArray = data.split(",")
        LoginEvent(dataArray(0).trim.toLong, dataArray(1).trim.toString, dataArray(2).trim.toString, dataArray(3).trim.toLong)
      })
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[LoginEvent](Time.seconds(5)) {
        //提取时间戳
        override def extractTimestamp(element: LoginEvent): Long = element.eventTime * 1000L //ｍs
      }) //乱序数据，给watermark延时,参数延迟时间：5s
      .keyBy(_.userId)

    //２. 定义匹配模式
    val loginFailPattern = Pattern.begin[LoginEvent]("begin").where(_.eventType == "fail")
      .next("next").where(_.eventType == "fail") //严格紧邻(连续失败)
      .within(Time.seconds(2)) //两秒内

    //３. 在事件流应用匹配模式，得到ＰatternＳtream对象
    val patternStream = CEP.pattern(loginEventStream, loginFailPattern)

    //４．从PatternStream中选择匹配事件序列，得到ＤataStream
    val loginFailDataStream = patternStream.select(new LoginFailMatch())

    loginFailDataStream.print()

    env.execute("login fail with cep job")


  }
}

//自定义匹配事件选择
class LoginFailMatch() extends PatternSelectFunction[LoginEvent,Warning]{
  //将检测到的所有事件序列保存成Ｍap,ｋey分别为:begin\next
  override def select(map: util.Map[String, util.List[LoginEvent]]): Warning = {
    //从map中按照名称取出对应的事件
    val firstFail=map.get("begin").iterator().next()
    val lastFail=map.get("next").iterator().next()
    Warning(firstFail.userId,firstFail.eventTime,lastFail.eventTime,"login fail!")

  }
}

