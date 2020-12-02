package com.wnswdwy.utils;

/**
 * @author yycstart
 * @create 2020-11-30 21:03
 */
public class RanOpt<T> {

    private T value;
    private int weight;

    public RanOpt(T value, int weight) {
        this.value = value;
        this.weight = weight;
    }

    public T getValue() {
        return value;
    }

    public int getWeight() {
        return weight;
    }
}
