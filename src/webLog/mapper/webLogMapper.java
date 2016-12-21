package webLog.mapper;

import config.parameter;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by Berger on 2016/12/14.
 */
public class webLogMapper extends Mapper<LongWritable, Text, Text, LongWritable> {
    LongWritable one = new LongWritable(1);


    public static String getEncoding(String str) {
        String encode = "GB2312";
        try {
            if (str.equals(new String(str.getBytes(encode), encode))) {      //判断是不是GB2312
                String s = encode;
                return s;      //是的话，返回“GB2312“，以下代码同理
            }
        } catch (Exception exception) {
        }
        encode = "ISO-8859-1";
        try {
            if (str.equals(new String(str.getBytes(encode), encode))) {      //判断是不是ISO-8859-1
                String s1 = encode;
                return s1;
            }
        } catch (Exception exception1) {
        }
        encode = "UTF-8";
        try {
            if (str.equals(new String(str.getBytes(encode), encode))) {   //判断是不是UTF-8
                String s2 = encode;
                return s2;
            }
        } catch (Exception exception2) {
        }
        encode = "GBK";
        try {
            if (str.equals(new String(str.getBytes(encode), encode))) {      //判断是不是GBK
                String s3 = encode;
                return s3;
            }
        } catch (Exception exception3) {
        }
        return "";        //如果都不是，说明输入的内容不属于常见的编码格式。
    }


    public void map(LongWritable ikey, Text ivalue, Context context) throws IOException, InterruptedException {
        String vString = ivalue.toString();
        String newStr = new String(vString.getBytes(), getEncoding(vString));
        String[] strings = newStr.split(parameter.SYMBOL);
        for (int i = 0; i < strings.length; i++) {
            switch (i){
                case 2:{
                    context.write(new Text(String.valueOf(i) + parameter.TAB + strings[i]), one);
                    break;
                }
                case 26:{
                    context.write(new Text(String.valueOf(i) + parameter.TAB + strings[i]), one);
                    break;
                }
                case 72:{
                    context.write(new Text(String.valueOf(i) + parameter.TAB + strings[i]), one);
                    break;
                }
            }

        }
    }
}