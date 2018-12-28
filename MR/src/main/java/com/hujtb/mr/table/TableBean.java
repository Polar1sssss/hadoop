package com.hujtb.mr.table;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @author hujtb
 * @create on 2018-12-21-16:40
 */
public class TableBean implements Writable{

    private String order_id;
    private String p_id;
    private String p_name;
    private int amount;
    private String flag;

    public TableBean() {
        super();
    }

    public TableBean(String order_id, String p_id, String p_name, int amount, String flag) {
        this.order_id = order_id;
        this.p_id = p_id;
        this.p_name = p_name;
        this.amount = amount;
        this.flag = flag;
    }

    public String getOrder_id() {
        return order_id;
    }

    public void setOrder_id(String order_id) {
        this.order_id = order_id;
    }

    public String getP_id() {
        return p_id;
    }

    public void setP_id(String p_id) {
        this.p_id = p_id;
    }

    public String getP_name() {
        return p_name;
    }

    public void setP_name(String p_name) {
        this.p_name = p_name;
    }

    public int getAmount() {
        return amount;
    }

    public void setAmount(int amount) {
        this.amount = amount;
    }

    public String getFlag() {
        return flag;
    }

    public void setFlag(String flag) {
        this.flag = flag;
    }

    //序列化
    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(order_id);
        dataOutput.writeUTF(p_id);
        dataOutput.writeUTF(p_name);
        dataOutput.writeInt(amount);
        dataOutput.writeUTF(flag);
    }

    //反序列化
    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.order_id = dataInput.readUTF();
        this.p_id = dataInput.readUTF();
        this.p_name = dataInput.readUTF();
        this.amount = dataInput.readInt();
        this.flag = dataInput.readUTF();
    }

    //控制输出的字段
    @Override
    public String toString() {
        return order_id + "\t" + p_name + "\t" + amount;
    }
}
