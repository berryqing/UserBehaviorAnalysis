package com.bigdata.hotitems_analysis.app

import com.bigdata.hotitems_analysis.bean.UserBehavior
import com.bigdata.hotitems_analysis.function.{CountAgg, WindowResult}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.windowing.time.Time

object HotItems {

    def main(args: Array[String]): Unit = {

        val env = StreamExecutionEnvironment.getExecutionEnvironment
        //从文件中读取出来，必然是统计数据产生时的时间，所以设置时间语义为EventTime
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
        env.setParallelism(1)

        val readFromFileStream = env.readTextFile("D:\\idea-workspace\\UserBehaviorAnalysis\\HotItemsAnalysis\\src\\main\\resources\\UserBehavior.csv")

        // 1. 将读取出来的文件转换为样例类
        val dataStream: DataStream[UserBehavior] = readFromFileStream
            .map { data =>
                val dataArr: Array[String] = data.split(",")
                UserBehavior(dataArr(0).toLong, dataArr(1).toLong, dataArr(2).toInt, dataArr(3), dataArr(4).toLong)
            }
            // 分配时间戳和watermark
            .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[UserBehavior](Time.seconds(1)) {
            override def extractTimestamp(t: UserBehavior): Long = t.timestamp * 1000L //当前时间是s，所以转为ms
        })


        // 2. 开窗聚合函数，以一个小时为窗口，5分钟为滑动时间
        // 过滤出用户的pv行为，然后keyby，然后开窗聚合
        val aggregatedStream = dataStream
            .filter(_.behavior == "pv")  //过滤出pv行为
            .keyBy(_.itemId)        //按照商品Id进行分组
            .timeWindow(Time.hours(1),Time.minutes(5))      //开滑动窗口
            .aggregate(new CountAgg(), new WindowResult()) //先做一个预聚合，然后传到window function中转换成加上窗口信息的包装类

        // 3. 排序输出topN
        //首先区分不同的窗口，按照不同的窗口划分组，在组内进行排序；
        //状态编程放在listState里面，设置定时器，等所有数据都到齐了才触发
        val resultStream = aggregatedStream
                .keyBy(_.windowEnd)  //按照窗口分组
                .process( new TopNItems(3))  //自定义process Function 做排序输出



        resultStream.print()
        env.execute()

    }

}
