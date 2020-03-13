import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector


object TxMatchByJoin {
  def main(args: Array[String]): Unit = {
    val env=StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)

    //读取支付到账事件流
    //val receiptResource=getClass.getResource("/ReceiptLog.csv")
    //val receiptEventStream=env.readTextFile(receiptResource.getPath)
    val receiptEventStream=env.readTextFile("OderPayDetect/src/main/resources/ReceiptLog.csv")
    //val receiptEventStream=env.socketTextStream("localhost",8888)
      .map(data=>{
        val dataArray=data.split(",")
        ReceiptEvent(dataArray(0).trim,dataArray(1).trim,dataArray(2).toLong)
      })
      .assignAscendingTimestamps(_.eventTime*1000L)//按升序处理
      .keyBy(_.txId)//按交易id分组

    //读取订单数据流，过滤有pay的信息
    val resource=getClass.getResource("/OrderLog.csv")
    val orderEventStream=env.readTextFile(resource.getPath)
    //val orderEventStream=env.socketTextStream("localhost",7777)
      .map(data=>{
        val dataArray=data.split(",")
        OrderEvent(dataArray(0).trim.toLong,dataArray(1).trim,dataArray(2).trim,dataArray(3).trim.toLong)
      })
      .filter(_.txId!="")//过滤有pay的信息
      .assignAscendingTimestamps(_.eventTime*1000L)
      .keyBy(_.txId)//根据支付id分组

    //Join 处理
    val processedStream=orderEventStream.intervalJoin(receiptEventStream)
      .between(Time.seconds(-5),Time.seconds(5))
      .process(new TxPayMatchByJoin())

    processedStream.print("Join")
    env.execute("tx pay match by Join job")

  }

}

class TxPayMatchByJoin() extends ProcessJoinFunction[OrderEvent,ReceiptEvent,(OrderEvent,ReceiptEvent)]{
  override def processElement(left: OrderEvent, right: ReceiptEvent, context: ProcessJoinFunction[OrderEvent, ReceiptEvent, (OrderEvent, ReceiptEvent)]#Context, collector: Collector[(OrderEvent, ReceiptEvent)]): Unit = {
    collector.collect((left,right))
  }
}
