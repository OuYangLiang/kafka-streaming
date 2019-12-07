package com.personal.oyl.sample.streaming.computer;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import java.io.Serializable;
import java.math.BigDecimal;
import java.util.HashSet;
import java.util.Set;

/**
 * @author OuYang Liang
 * @since 2019-09-04
 */
public class StatisticsAccumulator implements Serializable {
    private transient static final Gson gson = new GsonBuilder().setDateFormat("yyyy-MM-dd HH:mm:ss").create();

    private Long numOfOrders;
    private BigDecimal orderAmt;
    private Set<Integer> orderedCustId;

    public StatisticsAccumulator() {
        this.setNumOfOrders(0L);
        this.setOrderAmt(BigDecimal.ZERO);
        this.setOrderedCustId(new HashSet<>());
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

    public Set<Integer> getOrderedCustId() {
        return orderedCustId;
    }

    public void setOrderedCustId(Set<Integer> orderedCustId) {
        this.orderedCustId = orderedCustId;
    }

    public Statistics toStatistics() {
        Statistics statistics = new Statistics();
        statistics.setOrderAmt(this.getOrderAmt());
        statistics.setNumOfOrders(this.getNumOfOrders());
        if (null != this.getOrderedCustId()) {
            statistics.setNumOfOrderedCustomers(Long.valueOf(this.getOrderedCustId().size()));
        } else {
            statistics.setNumOfOrderedCustomers(0L);
        }
        return statistics;
    }

    public String json() {
        return gson.toJson(this);
    }

    public static StatisticsAccumulator fromJson(String json) {
        return gson.fromJson(json, StatisticsAccumulator.class);
    }
}
