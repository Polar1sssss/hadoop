package com.hujtb.mr.order;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * ·Ö×éÅÅĞòÀà
 * @author hujtb
 * @create on 2018-12-20-17:04
 */
public class OrderGroupingComparator extends WritableComparator {

    public OrderGroupingComparator(){
        super(OrderBean.class, true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        OrderBean aBean = (OrderBean)a;
        OrderBean bBean = (OrderBean)b;

        int result;

        if(aBean.getOrder_id() > bBean.getOrder_id()){
            result = 1;
        }else if(aBean.getOrder_id() < bBean.getOrder_id()){
            result = -1;
        }else{
            result = 0;
        }
        return result;
    }
}
