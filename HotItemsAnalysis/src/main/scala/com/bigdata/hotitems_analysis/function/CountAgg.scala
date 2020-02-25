package com.bigdata.hotitems_analysis.function

import com.bigdata.hotitems_analysis.bean.UserBehavior
import org.apache.flink.api.common.functions.AggregateFunction

class CountAgg extends AggregateFunction[UserBehavior,Long,Long] {
    //初始值
    override def createAccumulator(): Long = 0

    //每来一条数据
    override def add(in: UserBehavior, acc: Long): Long = acc + 1

    override def getResult(acc: Long): Long = acc

    override def merge(acc: Long, acc1: Long): Long = acc + acc1
}
