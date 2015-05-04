package com.yirendai.sqoop.common;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * Created by lihe on 4/29/15.
 * @author Li He
 */
public class Utils {
    public static String parseDate(Date date, String formatStr) {
        DateFormat format = new SimpleDateFormat(formatStr);
        return format.format(date);
    }
}
