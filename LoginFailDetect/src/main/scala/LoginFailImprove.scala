import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector

import scala.collection.mutable.ListBuffer

//输入登录事件数据样例类 [用户id,登录ip,返回类型(成功/失败),登录时间戳]
case class LoginEvent(userId:Long,ip:String,eventType:String,eventTime:Long)
//输出报警信息样例类
case class Warning(userId:Long,firstFailTime:Long,lastFailTime:Long,warning:String)

/**
 * 对LoginFail存储上次登录失败记录，与新接收的数据的时间戳比较，若小于2s，则输出报警信息。
 */
object LoginFailImprove {
  def main(args: Array[String]): Unit = {
    //1. 执行环境
    val env=StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)//采用的时间语义ＥventTime

    //读取事件数据
    val resource=getClass.getResource("/LoginLog.csv")
    val loginEventStream=env.readTextFile(resource.getPath)
      .map(data=>{
        val dataArray=data.split(",")
        LoginEvent(dataArray(0).trim.toLong,dataArray(1).trim.toString,dataArray(2).trim.toString,dataArray(3).trim.toLong)
      })
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[LoginEvent](Time.seconds(5)) {
        //提取时间戳
        override def extractTimestamp(element: LoginEvent): Long = element.eventTime*1000L //ｍs
      })//乱序数据，给watermark延时,参数延迟时间：5s

    val warningStream=loginEventStream
      .keyBy(_.userId)//以用户id分组
      .process(new LoginWarning(2))//传入失败次数,对每个分组执行LoginWarning()函数

    warningStream.print()
    env.execute("login fail ｉprove detect job")

  }
}

/**
 *
 * @param maxFailTimes
 */
class LoginWarning(maxFailTimes: Int) extends KeyedProcessFunction[Long,LoginEvent,Warning]{
  //定义状态，保存2s内所有登录失败事件,若遇到成功，清空该状态
  lazy val loginFailSate:ListState[LoginEvent]=getRuntimeContext.getListState(new ListStateDescriptor[LoginEvent]("login-fail-state",classOf[LoginEvent]))


  override def processElement(value: LoginEvent, context: KeyedProcessFunction[Long, LoginEvent, Warning]#Context, collector: Collector[Warning]): Unit = {
    if(value.eventType=="fail"){
      //判断之前是否有登录失败事件
      val iter=loginFailSate.get().iterator()

      //之前已经有登录失败事件
      if(iter.hasNext){
        val firstFail=iter.next()
        //比较时间戳，小于2s，报警（只能处理连续２个失败事件才报警,并且不能处理乱序数据）->使用cep
        if((value.eventTime-2)>firstFail.eventTime){
          collector.collect(Warning(value.userId,firstFail.eventTime,value.eventTime,"login fail in 2 seconds"))
        }
        //更新最近一次的登录失败事件，保存在状态中
        loginFailSate.clear()
        loginFailSate.add(value)
      }else{
        //如果是第一次登录失败，直接添加到状态
        loginFailSate.add(value)
      }

    }else{
      //到来的数据是登录成功，清空状态
      loginFailSate.clear()
    }
  }

}
