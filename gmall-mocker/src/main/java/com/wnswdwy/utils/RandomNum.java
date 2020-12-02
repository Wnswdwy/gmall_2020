package com.wnswdwy.utils;

/**
 * @author yycstart
 * @create 2020-11-30 21:03
 */
import java.util.Random;

public class RandomNum {
    public static int getRandInt(int fromNum, int toNum) {
        return fromNum + new Random().nextInt(toNum - fromNum + 1);
    }
}
