package game360Classification.reducer;

import config.parameter;
import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class game360ClassificationReducer extends Reducer<Text, Text, Text, LongWritable> {

    public void reduce(Text _key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        int a = 0;
        int b = 0;
        int c = 0;
        int d = 0;
        int e = 0;
        int f = 0;
        int g = 0;
        int h = 0;
        int i = 0;

        for (Text value : values) {
            String[] vs = value.toString().split(parameter.TAB);
            switch (Integer.parseInt(vs[0])) {
                case 1: {
                    a++;
                    break;
                }
                case 2: {
                    b++;
                    break;
                }
                case 3: {
                    c++;
                    break;
                }
                case 4: {
                    d++;
                    break;
                }
                case 5: {
                    e++;
                    break;
                }
                case 6: {
                    f++;
                    break;
                }
                case 7: {
                    g++;
                    break;
                }
                case 8: {
                    h++;
                    break;
                }
                case 9: {
                    i++;
                    break;
                }
            }
        }
        long sum = (long)(a+b+c+d+e+f+g+h+i);
        if (sum > 10) {
            context.write(new Text(_key.toString()+parameter.TAB+a+parameter.SLASH+b+parameter.SLASH+c+parameter.SLASH+d+parameter.SLASH+
                    e+parameter.SLASH+f+parameter.SLASH+g+parameter.SLASH+h+parameter.SLASH+i), new LongWritable(sum));
        }
    }
}