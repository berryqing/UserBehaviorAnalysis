package com.bigdata.hotitems_analysis.function

import com.bigdata.hotitems_analysis.bean.UserBehavior
import org.apache.flink.api.common.functions.AggregateFunction

/**
  * 求平均值 , 计算当前时间戳的平均值
  */
class AverageAgg extends AggregateFunction[UserBehavior,(Long,Int),Double] {

    //初始值
    override def createAccumulator(): (Long, Int) = (0L, 0)
    //每来一条数据，把时间戳添加到状态1中，把状态2的个数加1
    override def add(in: UserBehavior, acc: (Long, Int)): (Long, Int) = {
        (acc._1 + in.timestamp, acc._2 + 1)
        //当前状态(acc) + 输入数据(in)
    }
    //最终结果：平均值
    override def getResult(acc: (Long, Int)): Double = acc._1 / acc._2.toDouble
    //合并（各家各的，作用不大）
    override def merge(acc: (Long, Int), acc1: (Long, Int)): (Long, Int) = {
        (acc._1 + acc1._1, acc._2 + acc1._2)
    }
}
