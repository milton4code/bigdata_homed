package cn.ipanel.utils;

import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;

import static net.razorvine.pickle.PrettyPrint.print;

/**
 * 日期工具类
 */
public class MOEDateUtils {

    /**
     * 取得一年的第几周
     *
     * @param
     * @return
     */
    public static int getWeekOfYear(String time) {
        try {
            SimpleDateFormat sdf2 = new SimpleDateFormat("yyyyMMdd");
            Date date = sdf2.parse(time);
            Calendar c = Calendar.getInstance();
            c.setTime(date);
            int week_of_year = c.get(Calendar.WEEK_OF_YEAR);
            return week_of_year;
        } catch (Exception E) {
            E.printStackTrace();
            return 0;
        }
    }

    /**
     * 取得日期：月
     *
     * @param
     * @return
     */
    public static int getMonth(String time) {
        try {
            SimpleDateFormat sdf2 = new SimpleDateFormat("yyyyMMdd");
            Date date = sdf2.parse(time);
            Calendar c = Calendar.getInstance();
            c.setTime(date);
            int month = c.get(Calendar.MONTH);
            return month + 1;
        } catch (Exception E) {
            E.printStackTrace();
            return 0;
        }
    }

    /**
     * 1 第一季度 2 第二季度 3 第三季度 4 第四季度
     *
     * @param
     * @return
     */
    public static int getSeason(String time) {
        try {
            SimpleDateFormat sdf2 = new SimpleDateFormat("yyyyMMdd");
            Date date = sdf2.parse(time);
            int season = 0;
            Calendar c = Calendar.getInstance();
            c.setTime(date);
            int month = c.get(Calendar.MONTH);
            switch (month) {
                case Calendar.JANUARY:
                case Calendar.FEBRUARY:
                case Calendar.MARCH:
                    season = 1;
                    break;
                case Calendar.APRIL:
                case Calendar.MAY:
                case Calendar.JUNE:
                    season = 2;
                    break;
                case Calendar.JULY:
                case Calendar.AUGUST:
                case Calendar.SEPTEMBER:
                    season = 3;
                    break;
                case Calendar.OCTOBER:
                case Calendar.NOVEMBER:
                case Calendar.DECEMBER:
                    season = 4;
                    break;
                default:
                    break;
            }
            return season;
        } catch (Exception E) {
            E.printStackTrace();
            return 0;
        }
    }


    /**
     * 取得日期：年
     *
     * @param
     * @return
     */
    public static int getYear(String time) {
        try {
            SimpleDateFormat sdf2 = new SimpleDateFormat("yyyyMMdd");
            Date date = sdf2.parse(time);
            Calendar c = Calendar.getInstance();
            c.setTime(date);
            int year = c.get(Calendar.YEAR);
            return year;
        } catch (Exception E) {
            E.printStackTrace();
            return 0;
        }
    }

    /**
     * 获取某周得第一天
     *
     * @return
     */
    public static String getFirstOfWeek(String dataStr) throws ParseException {
        Calendar cal = Calendar.getInstance();
        cal.setTime(new SimpleDateFormat("yyyyMMdd").parse(dataStr));
        int d = 0;
        if (cal.get(Calendar.DAY_OF_WEEK) == 1) {
            d = -6;
        } else {
            d = 2 - cal.get(Calendar.DAY_OF_WEEK);
        }
        cal.add(Calendar.DAY_OF_WEEK, d);
        // 所在周开始日期
        String data1 = new SimpleDateFormat("yyyyMMdd").format(cal.getTime());
        return data1;

    }

    /**
     * 获取某周得最后一天
     *
     * @return
     */
    public static String getLastOfWeek(String dataStr) throws ParseException {
        Calendar cal = Calendar.getInstance();

        cal.setTime(new SimpleDateFormat("yyyyMMdd").parse(dataStr));

        int d = 0;
        if (cal.get(Calendar.DAY_OF_WEEK) == 1) {
            d = -6;
        } else {
            d = 2 - cal.get(Calendar.DAY_OF_WEEK);
        }
        cal.add(Calendar.DAY_OF_WEEK, d);
        // 所在周开始日期
        cal.add(Calendar.DAY_OF_WEEK, 6);
        // 所在周结束日期
        String data2 = new SimpleDateFormat("yyyyMMdd").format(cal.getTime());
        return data2;

    }

    /**
     * 获取某月得第一天
     *
     * @param year   年
     * @param months 月
     * @return
     */
    public static String getFirstDayOfMonth(int year, String months) {
        SimpleDateFormat sdf2 = new SimpleDateFormat("yyyyMMdd");
        Calendar c = new GregorianCalendar();
        int month = getMonth(months);
        month = month - 1;
        c.set(year, month, 1);
        return sdf2.format(c.getTime());
    }

    /**
     * 获取某月得最后一天
     *
     * @param year   年
     * @param months 月
     * @return
     */
    public static String getLastDayOfMonth(int year, String months) {
        SimpleDateFormat sdf2 = new SimpleDateFormat("yyyyMMdd");
        Calendar c = new GregorianCalendar();
        int month = getMonth(months);
        month = month - 1;
        c.set(year, month, 1);
        c.roll(Calendar.DATE, -1);
        return sdf2.format(c.getTime());
    }

    /**
     * 获取某季度得第一天
     *
     * @param year    　年
     * @param quarter 　季度
     * @return
     */
    public static String getFirstDayOfQuarter(int year, int quarter) {
        Calendar c = new GregorianCalendar();

        String month = "0";
        switch (quarter) {
            case 1:
                month = "1";
                break;
            case 2:
                month = "4";
                break;
            case 3:
                month = "7";
                break;
            case 4:
                month = "10";
                break;
            default:
                month = c.get(Calendar.MONTH) + "";
                break;
        }

        return getFirstDayOfMonth(year, month);
    }

    /**
     * 获取某季度得最一天
     *
     * @param year    　年
     * @param quarter 　季度
     * @return
     */
    public static String getLastDayOfQuarter(int year, int quarter) {
        Calendar c = new GregorianCalendar();

        String month = "0";
        switch (quarter) {
            case 1:
                month = "3";
                break;
            case 2:
                month = "6";
                break;
            case 3:
                month = "9";
                break;
            case 4:
                month = "12";
                break;
            default:
                month = c.get(Calendar.MONTH) + "";
                break;
        }

        return getLastDayOfMonth(year, month);
    }

    /**
     * 获取某年得第一天
     *
     * @param year 年
     * @return
     */
    public static String getFirstDayOfYear(int year) {
        return getFirstDayOfMonth(year, "1");
    }

    /**
     * 获取某年得最后一天
     *
     * @param year 年
     * @return
     */
    public static String getLastDayOfYear(int year) {
        return getLastDayOfMonth(year, "12");
    }

    public static DateTime dateStrToDateTime(String dateStr) {
        DateTimeFormatter pattern = DateTimeFormat.forPattern("YYYY_MM_DD_HHMMSS");
        return DateTime.parse(dateStr, pattern);
    }


    public static void main(String[] args) {
        String time = "20190102";
        try {
            print(getWeekOfYear(time));
            print(getMonth(time));
            print(getSeason(time));
            print(getYear(time));
        } catch (Exception E) {
            E.printStackTrace();
        }

    }

}