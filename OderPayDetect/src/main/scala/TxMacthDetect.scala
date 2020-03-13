import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.co.CoProcessFunction
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector



//订单输入样例类
//case class OrderEvent(orderId:Long,eventType:String,txId: String,eventTime:Long)
//交易接受流样例类
case class ReceiptEvent(txId:String,payChannel:String,eventTime:Long)

/**
 * 连接处理两个流
 */
object TxMacthDetect {

  //定义侧输出流,在订单数据流中有，支付到账流中没有
  val unmacthedPays=new OutputTag[OrderEvent]("unmacthedPays")
  //支付到账流中有，订单数据流中无
  val unmacthedReceipted=new OutputTag[ReceiptEvent]("unmacthedReceipted")


  def main(args: Array[String]): Unit = {
    val env=StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)

    //读取支付到账事件流
    //val receiptResource=getClass.getResource("/ReceiptLog.csv")
    //val receiptEventStream=env.readTextFile(receiptResource.getPath)
    //val receiptEventStream=env.readTextFile("OderPayDetect/src/main/resources/ReceiptLog.csv")
    val receiptEventStream=env.socketTextStream("localhost",8888)
      .map(data=>{
        val dataArray=data.split(",")
        ReceiptEvent(dataArray(0).trim,dataArray(1).trim,dataArray(2).toLong)
      })
      .assignAscendingTimestamps(_.eventTime*1000L)//按升序处理
      .keyBy(_.txId)//按交易id分组

    //读取订单数据流，过滤有pay的信息
    //val resource=getClass.getResource("/OrderLog.csv")
    //val orderEventStream=env.readTextFile(resource.getPath)
    val orderEventStream=env.socketTextStream("localhost",7777)
      .map(data=>{
        val dataArray=data.split(",")
        OrderEvent(dataArray(0).trim.toLong,dataArray(1).trim,dataArray(2).trim,dataArray(3).trim.toLong)
      })
      .filter(_.txId!="")//过滤有pay的信息
      .assignAscendingTimestamps(_.eventTime*1000L)
      .keyBy(_.txId)//根据支付id分组



    //将两条流连接，共同处理
    val processedStream=orderEventStream.connect(receiptEventStream)
      .process(new TxPayMacth())

    processedStream.print("matched")//匹配成功的
    processedStream.getSideOutput(unmacthedPays).print("unmacthedPays")
    processedStream.getSideOutput(unmacthedReceipted).print("unmacthedReceipted")

    env.execute("tx macth job")

  }

  //自定义流连接方法，将异常输入到侧输出流
  class TxPayMacth() extends CoProcessFunction[OrderEvent,ReceiptEvent,(OrderEvent,ReceiptEvent)]{
    //定义状态保存已经到达的订单状态和支付状态
    lazy val payState:ValueState[OrderEvent]=getRuntimeContext.getState(new ValueStateDescriptor[OrderEvent]("pay-state",classOf[OrderEvent]))
    lazy val receiptState:ValueState[ReceiptEvent]=getRuntimeContext.getState(new ValueStateDescriptor[ReceiptEvent]("receipt-state",classOf[ReceiptEvent]))


    //处理第一条流（订单事件流）
    override def processElement1(in1: OrderEvent, context: CoProcessFunction[OrderEvent, ReceiptEvent, (OrderEvent, ReceiptEvent)]#Context, collector: Collector[(OrderEvent, ReceiptEvent)]): Unit = {
      //判断有没有对应的到账事件　－>
      val receipt=receiptState.value()
      if(receipt!=null){
        //若已经有到账事件，在主流输出匹配信息
        collector.collect((in1,receipt))
        receiptState.clear()
      }else{
        //若到账事件还没来，将in1存入状态，注册定时器等待到账事件
        payState.update(in1)
        context.timerService().registerEventTimeTimer(in1.eventTime*1000L+5000L)//延迟5s(需要结合实际的订单流与支付流时间差)，多个流时，watermark选择最小的流的watermark作为本身watermark
      }
    }

    //处理第二条流元素（支付到账事件流）
    override def processElement2(in2: ReceiptEvent, context: CoProcessFunction[OrderEvent, ReceiptEvent, (OrderEvent, ReceiptEvent)]#Context, collector: Collector[(OrderEvent, ReceiptEvent)]): Unit = {
      val pay=payState.value()
      if(pay!=null){
        //ｐay不为空，则第一条流中订单来了（因为keyby为txId，只要第一个流中有数据，其txId一定与第二个流中当前数据的txId相等，两个流中为同一个txId）
        collector.collect((pay,in2))
        payState.clear()
      }else{
        //第一条流中数据没d到，等待
        receiptState.update(in2)
        context.timerService().registerEventTimeTimer(in2.eventTime*1000L+5000L)
      }
    }

    override def onTimer(timestamp: Long, ctx: CoProcessFunction[OrderEvent, ReceiptEvent, (OrderEvent, ReceiptEvent)]#OnTimerContext, out: Collector[(OrderEvent, ReceiptEvent)]): Unit = {
      //定时器触发，若还未收到事件，则报警
      if(payState.value()!=null){
        //ｒeceipt没来
        ctx.output(unmacthedPays,payState.value())
      }
      if(receiptState.value()!=null){
        ctx.output(unmacthedReceipted,receiptState.value())
      }
      payState.clear()
      receiptState.clear()
    }
  }

}

