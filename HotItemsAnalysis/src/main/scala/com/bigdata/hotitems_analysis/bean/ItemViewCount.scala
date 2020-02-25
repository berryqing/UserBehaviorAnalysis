package com.bigdata.hotitems_analysis.bean

/**
  * 定义中间聚合结果样例类
  * @param itemId
  * @param windowEnd
  * @param count
  */
case class ItemViewCount (itemId: Long,
                          windowEnd: Long,
                          count: Long
                         )
