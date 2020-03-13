import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

//定义输入订单事件样例类
//case class OrderEvent(orderId:Long,eventType:String,txId: String,eventTime:Long)
//定义输出结果样例类
//case class OrderResult(orderId:Long,resultMsg:String)

object OrderTimeoutWithoutCEP {
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

    //定义process function进行超时检测
    val timeoutWarningStream=orderEventStream.process(new OrderTimeoutWarning())
    timeoutWarningStream.print()

    env.execute("order timeout without cep job")

  }

}

//自定义事件处理函数
class OrderTimeoutWarning() extends KeyedProcessFunction[Long,OrderEvent,OrderResult]{
  //保存payed状态是否来过
  lazy val isPayedState:ValueState[Boolean]=getRuntimeContext.getState(new ValueStateDescriptor[Boolean]("isPayed-state",classOf[Boolean]))
  //来一条数据处理一次
  override def processElement(value: OrderEvent, context: KeyedProcessFunction[Long, OrderEvent, OrderResult]#Context, collector: Collector[OrderResult]): Unit = {
    //取出状态标志位
    val isPayed=isPayedState.value()
    if(value.eventType=="create" && !isPayed){
      //若遇到"create"事件，且"pay"没来过,注册定时器
      context.timerService().registerEventTimeTimer(value.eventTime*1000L+15*60*1000L)//定时器15分钟
    }else if(value.eventType=="pay"){
      //若是pay事件，则修改状态为true
      isPayedState.update(true)
    }
  }


  //定时器触发事件，15分钟后才会输出结果
  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, OrderEvent, OrderResult]#OnTimerContext, out: Collector[OrderResult]): Unit = {
    //判断isPayed状态，若为true，则成功支付，若为false，则报警
    val isPayed=isPayedState.value()
    if(isPayed){
      out.collect(OrderResult(ctx.getCurrentKey,"order payed successfully"))
    }else{
      out.collect(OrderResult(ctx.getCurrentKey,"order timeout"))
    }
    //清空状态
    isPayedState.clear()
  }
}
