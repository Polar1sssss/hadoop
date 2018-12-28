package com.hujtb.mr.order;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @author hujtb
 * @create on 2018-12-20-15:43
 */
public class OrderBean implements WritableComparable<OrderBean> {

    private int order_id;
    private double price;

    public OrderBean() {}

    public OrderBean(int order_id, double price) {
        this.order_id = order_id;
        this.price = price;
    }

    public int getOrder_id() {
        return order_id;
    }

    public void setOrder_id(int order_id) {
        this.order_id = order_id;
    }

    public double getPrice() {
        return price;
    }

    public void setPrice(double price) {
        this.price = price;
    }

    @Override
    public int compareTo(OrderBean o) {
        //先按照订单升序排序，相同则按照价格降序排序
        int result;
        if(this.order_id > o.getOrder_id()){
            result = 1;
        }else if(this.order_id < o.getOrder_id()){
            result = -1;
        }else{
            if(this.price > o.getPrice()){
                result = -1;
            }else if(this.price < o.getPrice()){
                result = -1;
            }else{
                result = 0;
            }
        }
        return result;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeInt(order_id);
        dataOutput.writeDouble(price);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        order_id = dataInput.readInt();
        price = dataInput.readDouble();
    }

    @Override
    public String toString() {
        return order_id + "\t" +  price;
    }
}
