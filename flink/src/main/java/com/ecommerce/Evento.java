package com.ecommerce;

import java.time.Instant;

public class Evento {

    private String timestamp;
    private int user_id;
    private String event_type;
    private int product_id;
    private String product_name;
    private double price;

    public Evento() {
    }

    public String getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(String timestamp) {
        this.timestamp = timestamp;
    }

    public int getUser_id() {
        return user_id;
    }

    public void setUser_id(int user_id) {
        this.user_id = user_id;
    }

    public String getEvent_type() {
        return event_type;
    }

    public void setEvent_type(String event_type) {
        this.event_type = event_type;
    }

    public int getProduct_id() {
        return product_id;
    }

    public void setProduct_id(int product_id) {
        this.product_id = product_id;
    }

    public String getProduct_name() {
        return product_name;
    }

    public void setProduct_name(String product_name) {
        this.product_name = product_name;
    }

    public double getPrice() {
        return price;
    }

    public void setPrice(double price) {
        this.price = price;
    }

    public long getTimestampMillis() {
        return Instant.parse(timestamp).toEpochMilli();
    }

    @Override
    public String toString() {
        return "Evento{" +
                "timestamp='" + timestamp + '\'' +
                ", user_id=" + user_id +
                ", event_type='" + event_type + '\'' +
                ", product_id=" + product_id +
                ", product_name='" + product_name + '\'' +
                ", price=" + price +
                '}';
    }
}
