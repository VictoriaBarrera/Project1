import java.io.IOException;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class GenProgMapper extends Mapper<Object, Text, Text, DoubleWritable> {
    private final static DoubleWritable result = new DoubleWritable();
    private Text gender = new Text();

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        // Skip the header line
        if (key.equals(0)) {
            return;
        }

        // Split the line by comma
        String[] fields = value.toString().split(",");

        try {
            // Assuming CGPA is the 5th column (adjust index as needed)
            String genderValue = fields[2].trim();
            String cgpaValue = fields[4].trim();

            // Convert CGPA to double
            double cgpa = Double.parseDouble(cgpaValue);

            // Set the gender and CGPA
            gender.set(genderValue);
            result.set(cgpa);

            // Write to context
            context.write(gender, result);

        } catch (NumberFormatException e) {
            // Skip invalid rows or log them
            System.err.println("Skipping invalid row: " + value);
        }
    }
}
