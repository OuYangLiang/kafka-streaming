package com.personal.oyl.sample.stream.sink;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import java.io.Serializable;
import java.math.BigDecimal;
import java.util.HashSet;
import java.util.Set;

/**
 * @author OuYang Liang
 * @since 2019-09-03
 */
public class Statistics implements Serializable {

    private transient static final Gson gson = new GsonBuilder().setDateFormat("yyyy-MM-dd HH:mm:ss").create();

    private String minute;
    private Long numOfOrders;
    private BigDecimal orderAmt;
    private Long numOfOrderedCustomers;
    private Set<Integer> orderedCustId;

    public Statistics() {
        this.setNumOfOrders(0L);
        this.setOrderAmt(BigDecimal.ZERO);
        this.orderedCustId = new HashSet<>();
    }

    public String getMinute() {
        return minute;
    }

    public void setMinute(String minute) {
        this.minute = minute;
    }

    public Long getNumOfOrders() {
        return numOfOrders;
    }

    public void setNumOfOrders(Long numOfOrders) {
        this.numOfOrders = numOfOrders;
    }

    public BigDecimal getOrderAmt() {
        return orderAmt;
    }

    public void setOrderAmt(BigDecimal orderAmt) {
        this.orderAmt = orderAmt;
    }

    public Long getNumOfOrderedCustomers() {
        if (null == numOfOrderedCustomers) {
            return (long) this.orderedCustId.size();
        } else {
            return numOfOrderedCustomers;
        }
    }

    public void setNumOfOrderedCustomers(Long numOfOrderedCustomers) {
        this.numOfOrderedCustomers = numOfOrderedCustomers;
    }

    public void addCust(int custId) {
        this.orderedCustId.add(custId);
    }

    public String json() {
        return gson.toJson(this);
    }

    public static Statistics fromJson(String json) {
        return gson.fromJson(json, Statistics.class);
    }
}
