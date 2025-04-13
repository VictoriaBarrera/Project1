import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;

public class GenProgReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {

    @Override
    protected void reduce(Text key, Iterable<DoubleWritable> values, Context context)
            throws IOException, InterruptedException {
        double sum = 0;
        int count = 0;

        // Iterate through the values and calculate the sum and count
        for (DoubleWritable val : values) {
            sum += val.get();
            count++;
        }

        double avg = (count == 0) ? 0 : sum / count;  // Avoid division by zero
        context.write(key, new DoubleWritable(avg));  // Write the result (key, average CGPA)
    }
}
