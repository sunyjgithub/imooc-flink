/*******************************************************************************
 * @(#)MyBehavior.java 2021/9/7
 *
 * Copyright 2021 emrubik Group Ltd. All rights reserved.
 * EMRubik PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 *******************************************************************************/
package com.atguigu.flink.apitest;

import java.sql.Timestamp;

/**
 * 这里请补充该类型的简述说明
 * MyBehavior → 解析Json字符串后生成的JavaBean
 *
 * @author <a href="mailto:person@emrubik.com">sunyj</a>
 * @version $$Revision 1.0 $$ 2021/9/7 20:13
 */
public class MyBehavior {

    // 用户ID
    public String userId;
    // 商品ID
    public String itemId;
    // 商品类目ID
    public String categoryId;
    // 用户行为, 包括("pv", "buy", "cart", "fav")
    public String type;
    // 行为发生的时间戳，单位秒
    public long timestamp;

    public long counts = 1;

    public static MyBehavior of(String userId, String itemId, String categoryId, String type, long timestamp) {
        MyBehavior behavior = new MyBehavior();
        behavior.userId = userId;
        behavior.itemId = itemId;
        behavior.categoryId = categoryId;
        behavior.type = type;
        behavior.timestamp = timestamp;
        return behavior;
    }

    public static MyBehavior of(String userId, String itemId, String categoryId, String type, long timestamp,
                                long counts) {
        MyBehavior behavior = new MyBehavior();
        behavior.userId = userId;
        behavior.itemId = itemId;
        behavior.categoryId = categoryId;
        behavior.type = type;
        behavior.timestamp = timestamp;
        behavior.counts = counts;
        return behavior;
    }

    @Override
    public String toString() {
        return "MyBehavior{" + "userId='" + userId + '\'' + ", itemId='" + itemId + '\''
                + ", categoryId='" + categoryId + '\'' + ", type='" + type + '\''
                + ", timestamp=" + timestamp + "," + new Timestamp(timestamp)
                + "counts=" + counts + '}';
    }

    public String getUserId() {
        return userId;
    }
    public String getItemId() {
        return itemId;
    }
    public String getCategoryId() {
        return categoryId;
    }
    public String getType() {
        return type;
    }
    public long getTimestamp() {
        return timestamp;
    }
    public long getCounts() {
        return counts;
    }
}
