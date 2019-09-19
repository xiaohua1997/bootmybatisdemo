package com.moumou.bootmybatisdemo.dataAlignment.common;

public class StringExtension {
    public static String toStyleString(String in) {
        if(null != in) {
            return in.toLowerCase();
        } else {
            return null;
        }
    }
}
