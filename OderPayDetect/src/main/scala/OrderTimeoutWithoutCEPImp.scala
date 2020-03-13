import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector




//定义输入订单事件样例类
//case class OrderEvent(orderId:Long,eventType:String,txId: String,eventTime:Long)
//定义输出结果样例类
//case class OrderResult(orderId:Long,resultMsg:String)



/**
 * 对OrderTimeoutWithoutCEP的改进，支付成功的放入主流，超时的放进侧输出流
 */
object OrderTimeoutWithoutCEPImp {

  val orderTimeoutOutputTag=new OutputTag[OrderResult]("orderTimeout")


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
    val orderResultStream=orderEventStream.process(new OrderPayMatch())
    orderResultStream.print("payed")//输出主流　
    orderResultStream.getSideOutput(orderTimeoutOutputTag).print("timeout")//输出侧输出流

    env.execute("order timeout without cep job")

  }

  //可以处理create＼pay乱序到来　（由于已经通过orderId分组，所以来的都是id相等的）
  class OrderPayMatch() extends KeyedProcessFunction[Long,OrderEvent,OrderResult]{
    //保存payed状态是否来过
    lazy val isPayedState:ValueState[Boolean]=getRuntimeContext.getState(new ValueStateDescriptor[Boolean]("isPayed-state",classOf[Boolean]))
    //保存定时器时间戳为状态，有定时器时，表明create来过了
    lazy val timeState:ValueState[Long]=getRuntimeContext.getState(new ValueStateDescriptor[Long]("timer-state",classOf[Long]))

    override def processElement(value: OrderEvent, context: KeyedProcessFunction[Long, OrderEvent, OrderResult]#Context, collector: Collector[OrderResult]): Unit = {
      val isPayed=isPayedState.value()
      val timerTs=timeState.value()

      //根据事件类型判断处理
      if(value.eventType=="create"){
        //若是＂create＂，判断＂pay＂是否来过
        if(isPayed){
          //pay来过，匹配成功，清空状态,删除定时器
          collector.collect(OrderResult(value.orderId,"payed successfully")) //实时输出成功信息
          context.timerService().deleteEventTimeTimer(timerTs)//删除定时器
          isPayedState.clear()
          timeState.clear()
        }else{
          //若没有pay过，注册定时器等待pay的到来
          val ts=value.eventTime*1000L+15*60+1000L
          context.timerService().registerEventTimeTimer(ts)
          timeState.update(ts)//保存create+延迟时间
        }
      }else if(value.eventType=="pay"){
        //若是pay来了，则判断create是否来过了
        if(timerTs>0){
          //若有定时器，说明create来过
          //继续判断是否超过timeout时间
          if(timerTs > value.eventTime*1000L){
            //定时器时间未到，输出成功支付
            collector.collect(OrderResult(value.orderId,"payed successfully"))//成功信息输出到主流  实时输出成功信息

          }else{
            //如果pay到来时超时，输出到侧输出流
            context.output(orderTimeoutOutputTag,OrderResult(value.orderId,"payed but already timeout"))
          }
          collector.collect(OrderResult(value.orderId,"payed successfully"))
          context.timerService().deleteEventTimeTimer(timerTs)//删除定时器
          isPayedState.clear()
          timeState.clear()
        }else{
          //pay先到了,更新状态，等下create（注册定时器）
          isPayedState.update(true)
          context.timerService().registerEventTimeTimer(value.eventTime*1000L)//当前数据eventTime作为定时器触发时间
          timeState.update(value.eventTime*1000L)
        }

      }
    }

    //create没等到pay触发＼pay没等到create触发
    override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, OrderEvent, OrderResult]#OnTimerContext, out: Collector[OrderResult]): Unit = {
      //根据状态的值，判断哪个数据先来
      if(timeState.value()>0){
        //create先来，没等到pay
        ctx.output(orderTimeoutOutputTag,OrderResult(ctx.getCurrentKey,"time out"))
      }else{
        //pay先来,没等到create,侧输出流输出
        ctx.output(orderTimeoutOutputTag,OrderResult(ctx.getCurrentKey,"already payed but not found create log"))
      }
      isPayedState.clear()
      timeState.clear()
    }
  }

}

