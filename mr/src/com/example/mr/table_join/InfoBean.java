package com.example.mr.table_join;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class InfoBean implements Writable {


    private int orderId;
    private String dateString;
    private String pId;
    private int amount;
    private String pName;
    private int categoryId;
    private float price;
    private String flag;


    public void set(int orderId, String dateString, String pId, int amount, String pName, int categoryId, float price, String flag) {
        this.orderId = orderId;
        this.dateString = dateString;
        this.pId = pId;
        this.amount = amount;
        this.pName = pName;
        this.categoryId = categoryId;
        this.price = price;
        this.flag = flag;
    }

    public int getOrderId() {
        return orderId;
    }

    public void setOrderId(int orderId) {
        this.orderId = orderId;
    }

    public String getDateString() {
        return dateString;
    }

    public void setDateString(String dateString) {
        this.dateString = dateString;
    }

    public String getpId() {
        return pId;
    }

    public void setpId(String pId) {
        this.pId = pId;
    }

    public int getAmount() {
        return amount;
    }

    public void setAmount(int amount) {
        this.amount = amount;
    }

    public String getpName() {
        return pName;
    }

    public void setpName(String pName) {
        this.pName = pName;
    }

    public int getCategoryId() {
        return categoryId;
    }

    public void setCategoryId(int categoryId) {
        this.categoryId = categoryId;
    }

    public float getPrice() {
        return price;
    }

    public void setPrice(float price) {
        this.price = price;
    }

    public String getFlag() {
        return flag;
    }

    public void setFlag(String flag) {
        this.flag = flag;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {

        dataOutput.writeInt(orderId);
        dataOutput.writeUTF(dateString);
        dataOutput.writeUTF(pId);
        dataOutput.writeInt(amount);
        dataOutput.writeUTF(pName);
        dataOutput.writeInt(categoryId);
        dataOutput.writeFloat(price);
        dataOutput.writeUTF(flag);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.orderId = dataInput.readInt();
        this.dateString = dataInput.readUTF();
        this.pId = dataInput.readUTF();
        this.amount = dataInput.readInt();
        this.pName = dataInput.readUTF();
        this.categoryId = dataInput.readInt();
        this.price = dataInput.readFloat();
        this.flag = dataInput.readUTF();
    }

    @Override
    public String toString() {
        return "InfoBean{" +
                "orderId=" + orderId +
                ", dateString='" + dateString + '\'' +
                ", pId='" + pId + '\'' +
                ", amount=" + amount +
                ", pName='" + pName + '\'' +
                ", categoryId=" + categoryId +
                ", price=" + price +
                ", flag='" + flag + '\'' +
                '}';
    }


}
