package com.bigdata.hotitems_analysis.function

import java.sql.Timestamp

import com.bigdata.hotitems_analysis.bean.ItemViewCount
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.util.Collector

import scala.collection.mutable.ListBuffer

class TopNItems(topSize: Int) extends KeyedProcessFunction[Long,ItemViewCount,String] {

    // 先定义一个列表状态，用来保存当前窗口中所有item的count聚合结果
    private var itemListState: ListState[ItemViewCount] = _


    override def open(parameters: Configuration): Unit = {
        //获得真正的列表状态，拿到状态后待以后使用
        itemListState = getRuntimeContext.getListState(new ListStateDescriptor[ItemViewCount]("item-list",classOf[ItemViewCount]))

    }

    override def processElement(i: ItemViewCount,
                                context: KeyedProcessFunction[Long, ItemViewCount, String]#Context,
                                collector: Collector[String]): Unit = {
        // 每条数据来了之后保存入状态，第一条数据来的时候，注册一个定时器，在windowEnd之后延迟1ms
        itemListState.add(i) //把来的数据添加到状态
        // 可以直接每个数据都注册一个定时器，因为定时器是以时间戳作为id的，同样时间戳的定时器不会重复注册和触发
        context.timerService().registerEventTimeTimer(i.windowEnd + 1) //注册定时器

    }

    // 等到watermark超过windowEnd，达到windowEnd+1，说明所有聚合结果都到齐了，排序输出
    override def onTimer(timestamp: Long,
                         ctx: KeyedProcessFunction[Long, ItemViewCount, String]#OnTimerContext,
                         out: Collector[String]): Unit = {

        //1. 把数据从itemListState中取出来，放到List中，这样方便排序
        val allItemsList: ListBuffer[ItemViewCount] = ListBuffer()

        import scala.collection.JavaConversions._
        for (item <- itemListState.get()) {
            allItemsList += item
        }

        //提取完数据之后，就可以清空状态了
        itemListState.clear()

        //2. 按照count大小进行排序,降序
        val sortedItemsList = allItemsList.sortBy(_.count)(Ordering.Long.reverse).take(topSize)


        //3. 将排序后列表中的信息，格式化之后输出
        val result: StringBuilder = new StringBuilder
        result.append("==================================\n")
        result.append("窗口关闭时间：").append( new Timestamp(timestamp - 1) ).append("\n")
        // 用一个for循环遍历sortedList，输出前三名的所有信息
        for( i <- sortedItemsList.indices){
            val currentItem = sortedItemsList(i)
            result.append("No").append( i + 1 ).append(":")
                .append(" 商品ID=").append(currentItem.itemId)
                .append(" 浏览量=").append(currentItem.count)
                .append("\n")
        }
        // 控制显示频率
        Thread.sleep(1000L)
        // 最终输出结果
        out.collect(result.toString())


    }
}
