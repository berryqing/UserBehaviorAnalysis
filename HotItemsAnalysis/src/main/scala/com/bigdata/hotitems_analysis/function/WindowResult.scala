package com.bigdata.hotitems_analysis.function

import com.bigdata.hotitems_analysis.bean.ItemViewCount
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

/**
  *  该函数的输入就是预聚合函数的输出
  *  trait WindowFunction[IN, OUT, KEY, W <: org.apache.flink.streaming.api.windowing.windows.Window]
  */
class WindowResult extends WindowFunction[Long,ItemViewCount,Long,TimeWindow]{

    override def apply(key: Long,   //当前的key
                       window: TimeWindow,  //当前的window信息
                       input: Iterable[Long], //count
                       out: Collector[ItemViewCount] // out用来输出
                      ): Unit = {

        val itemId: Long = key
        val windowEnd: Long = window.getEnd
        val count: Long = input.iterator.next()
        out.collect(ItemViewCount(itemId,windowEnd,count))

    }
}
