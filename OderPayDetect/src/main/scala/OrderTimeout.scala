
import java.util

import org.apache.flink.cep.{PatternSelectFunction, PatternTimeoutFunction}
import org.apache.flink.cep.scala.CEP
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._

//定义输入订单事件样例类
case class OrderEvent(orderId:Long,eventType:String,txId: String,eventTime:Long)
//定义输出结果样例类
case class OrderResult(orderId:Long,resultMsg:String)



object OrderTimeout {
  def main(args: Array[String]): Unit = {
    val env=StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)

    //读取订单数据
    val resource=getClass.getResource("/OrderLog.csv")
    val orderEventStream=env.readTextFile(resource.getPath)
      .map(data=>{
        val dataArray=data.split(",")
        OrderEvent(dataArray(0).trim.toLong,dataArray(1).trim,dataArray(2).trim,dataArray(3).trim.toLong)
      })
      .assignAscendingTimestamps(_.eventTime*1000L)
      .keyBy(_.orderId)

    //定义匹配模式
    val orderPayPattern=Pattern.begin[OrderEvent]("begin").where(_.eventType=="create")
      .followedBy("follow").where(_.eventType=="pay")//非严格紧邻
      .within(Time.minutes(15))

    //将模式应用于数据流,可以提取正常事件＼超时事件
    val patternStream=CEP.pattern(orderEventStream,orderPayPattern)

    //调用select，提取超时事件并报警-＞侧输出流输出超时事件
    val orderTimeoutOutputTag=new OutputTag[OrderResult]("orderTimeout")

    //可以输出侧输出流
    val resultStream=patternStream.select(orderTimeoutOutputTag,
      new OrderTimeoutSelect(),
      new OrderPaySelect())
    resultStream.print("payed")//支付结果
    resultStream.getSideOutput(orderTimeoutOutputTag).print("timeout")//从侧输出流得到超时结果
    env.execute("order timeout job!")


  }
}

//超时事件序列的处理方法
class OrderTimeoutSelect() extends PatternTimeoutFunction[OrderEvent,OrderResult]{
  //超时事件，key:ｂegin,没有follow  ,其中l为超时时间戳
  override def timeout(map: util.Map[String, util.List[OrderEvent]], l: Long): OrderResult = {
    val timeoutOrderId=map.get("begin").iterator().next().orderId
    OrderResult(timeoutOrderId,"timeout")

  }
}

//自定义正常支付事件处理函数
class OrderPaySelect() extends PatternSelectFunction[OrderEvent,OrderResult]{
  override def select(map: util.Map[String, util.List[OrderEvent]]): OrderResult = {
    val payOrderId=map.get("begin").iterator().next().orderId
    OrderResult(payOrderId,"payed success")
  }
}
