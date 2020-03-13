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
 * 按照用户ID分流，遇到登陆失败事件时将其保存到ListState中(状态编程)，并注册定时器，设置2s后触发。
 * 定时器触发时检查状态中的登录失败事件个数，若大于等于2，就输出报警信息
 */
object LoginFail {
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
    env.execute("login fail detect job")
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
    //获取登录失败状态列表
    val loginFailList=loginFailSate.get()

    //判断类型是否是Fail,若是失败，添加到状态，若是成功，清空状态及定时器(由于之前已经按照userId分组，所以来的数据都是同一用户的登录数据)
    if(value.eventType=="fail"){
      //若状态列表为空,注册定时器
      if(!loginFailList.iterator().hasNext) {
        //注册定时器,当前数据时间２s后触发定时器
        context.timerService().registerEventTimeTimer(value.eventTime*1000L+2000L)

      }
      loginFailSate.add(value)
    }else{
      //如果是登录成功，清空状态
      loginFailSate.clear()
    }
  }

  //定时器触发动作
  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, LoginEvent, Warning]#OnTimerContext, out: Collector[Warning]): Unit = {
    //根据状态列表里数目决定是否输出报警信息
    val allLoginFails:ListBuffer[LoginEvent]=new ListBuffer[LoginEvent]()
    val iter=loginFailSate.get().iterator()//状态的迭代器
    //遍历迭代器访问状态数据,存入列表对象中
    while(iter.hasNext){
      allLoginFails+=iter.next()
    }
    //判断个数,并输出报警信息
    if(allLoginFails.length>=maxFailTimes){
      out.collect(Warning(allLoginFails.head.userId,allLoginFails.head.eventTime,allLoginFails.last.eventTime,"login fail in 2 seconds "+allLoginFails.length+" times"))
    }

    //清空状态
    allLoginFails.clear()
  }
}



