/*******************************************************************************
 * @(#)ItemViewCount.java 2021/9/7
 *
 * Copyright 2021 emrubik Group Ltd. All rights reserved.
 * EMRubik PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 *******************************************************************************/
package com.atguigu.flink.apitest;

import java.sql.Timestamp;

/**
 * 这里请补充该类型的简述说明
 *
 * @author <a href="mailto:person@emrubik.com">sunyj</a>
 * @version $$Revision 1.0 $$ 2021/9/7 20:15
 */
public class ItemViewCount {

    // 商品ID
    public String itemId;
    // 事件类型
    public String type;
    // 窗口开始时间戳
    public long windowStart;
    // 窗口结束时间戳
    public long windowEnd;
    // 商品的点击量
    public long viewCount;

    public static ItemViewCount of(String itemId, String type, long windowStart, long windowEnd, long viewCount) {
        ItemViewCount result = new ItemViewCount();
        result.itemId = itemId;
        result.type = type;
        result.windowStart = windowStart;
        result.windowEnd = windowEnd;
        result.viewCount = viewCount;
        return result;
    }

    @Override
    public String toString() {
        return "{" +
                "itemId='" + itemId + '\'' +
                "type='" + type + '\'' +
                ", windowStart=" + windowStart + " , " + new Timestamp(windowStart) +
                ", windowEnd=" + windowEnd + " , " + new Timestamp(windowEnd) +
                ", viewCount=" + viewCount +
                '}';
    }
}
