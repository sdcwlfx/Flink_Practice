import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.triggers.{Trigger, TriggerResult}
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector
import redis.clients.jedis.Jedis


//输入样例类
case class UserBehavior(userId:Long,itemId:Long,categoryId:Int,behavior:String,timestamp:Long)
//输出样例类
case class UvCount(windowEnd:Long,count:Long)


/**
 * 布隆过滤器，使用位图存储(实现哈希判断键值是否存在)
 * 特点：高效插入和查询
 * 缺点：返回的结果是概率性的而不是确切的，只能说明“某键值一定不存在或者可能存在”
 */

//由于数据可能很大，数据放在redis中而非内存，读取redis中数据
object UvWithBloom {
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
      .map(data=>("dummyKey",data.userId))//需要对userId进行Ｈash
      .keyBy(_._1)//以"dummyKey"子段分组(所有数据均在一组)
      .timeWindow(Time.hours(1))
      .trigger(new MyTrigger())//不要窗口存储完数据关闭后才处理，这样数据就又都存储在内存了，使用其来一条数据触发一次窗口操作
      .process(new UvCountWithBloom())//触发窗口执行的动作

    dataStream.print()

    env.execute("uv with bloom job")
  }
}



//自定义触发器，使得每来一条数据就触发一次窗口操作
/**
 * 输入类型:(String,Long)二元组
 * window类型:TimeWindow
 */
class MyTrigger() extends Trigger[(String,Long),TimeWindow]{
  //EventTime时间语义触发动作，由watermark触发(新来的数据时间戳大于等于窗口结束时间时，执行此操作)
  override def onEventTime(l: Long, w: TimeWindow, triggerContext: Trigger.TriggerContext): TriggerResult =TriggerResult.CONTINUE

  override def onProcessingTime(l: Long, w: TimeWindow, triggerContext: Trigger.TriggerContext): TriggerResult = TriggerResult.CONTINUE

  override def clear(w: TimeWindow, triggerContext: Trigger.TriggerContext): Unit = {}

  //每个元素来的时候触发窗口操作并清空所有窗口状态(不然一直保存状态，内存撑爆)
  override def onElement(t: (String, Long), l: Long, w: TimeWindow, triggerContext: Trigger.TriggerContext): TriggerResult = {
    TriggerResult.FIRE_AND_PURGE //触发窗口操作并清空状态
  }
}



//自定义布隆过滤器
class Bloom(size:Long) extends Serializable{
  //位图的大小
  private val cap=if (size>0) size else 1<<27 //能存储１６MB的位图
  //定义Ｈａｓh函数,seed为随机种子
  def hash(value:String,seed:Int):Long={
    var result:Long=0L
    //遍历value的每一位，应用算法进行叠加
    for(i<-0 until value.length ){
      result=result*seed+value.charAt(i)
    }
    result & (cap-1)//截取后27位

  }
}





//自定义窗口触发执行函数
/**
 * 输入类型:(String,Long)
 * 输出类型:UvCount
 * key类型:String
 * 窗口类型：TimeWindow
 */
class UvCountWithBloom() extends ProcessWindowFunction[(String,Long),UvCount,String,TimeWindow]{
  //定义redis连接
  lazy val jedis=new Jedis("localhost",6379)
  lazy val bloom=new Bloom(1<<29)//定义 ６４Ｍ大小位图，能处理５亿个key的量级

  //MyTrgger指定每来一条数据就触发一次该函数，elements为窗口中所有数据,由于Ｔrigger清空了窗口，所有窗口中只有一条新到来的数据
  override def process(key: String, context: Context, elements: Iterable[(String, Long)], out: Collector[UvCount]): Unit = {
    //每个窗口必须单独一个位图，用户在该窗口时间内访问不影响其在下个时间窗口的计数，故位图存储方式：key＝ｗindowEnd, value=bitmap
    val storeKey=context.window.getEnd.toString //窗口结束时间
    var count=0L//UV计数，由于上面Ｔriiger中每来一条数据就清空了状态，所以无法使用状态编程将其放入内存，所以放在redis中
    //把每个窗口的uv ｃount值存入redis表中，所以每次要先从redis中读取该值并更新该值(若新来的数据位图中没有，则count+1，并写入位图，位图用来去重的，位图中１的位数＝count)
    //ｒedis表中内容为(windowEnd,count)
    if(jedis.hget("count",storeKey)!=null){//从redis表"count"中获取键为“storeKey”值对应的count值
      count=jedis.hget("count",storeKey).toLong

    }
    //用布隆过滤器判断当前用户是否在该窗口中已经存在，若不存在，则写入该时间窗口对应的位图中，并更新count值，回写入redis表"count"中
    val userId=elements.last._2.toString
    val offset=bloom.hash(userId,61)//ｕserId在位图中的偏移量

    //定义标志位，判断redis该时间窗口对应的位图中是否有该位(是否已经存在该userId)
    val isExist=jedis.getbit(storeKey,offset)//从key=storeKey(制定时间窗口)的位图的offset位置取出位值
    if(!isExist){
      //如果不存在，位图对应位置置１，count+1
      jedis.setbit(storeKey,offset,true)//位图作用：判断新来的userId是否在该时间窗口已经存在, key=windowEnd
      jedis.hset("count",storeKey,(count+1).toString)//“count”表作用：存储不同时间窗口内ＵＶ的数量，ｋey=windowEnd
      out.collect(UvCount(storeKey.toLong,count+1))
    }else{
      out.collect(UvCount(storeKey.toLong,count))
    }
  }

}


