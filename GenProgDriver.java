import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import org.apache.hadoop.fs.Path;


public class GenProgDriver {

    public static void main(String[] args) throws Exception {

        // Create a Job instance
        Job job = Job.getInstance();
        job.setJarByClass(GenProgDriver.class);

        // Set Mapper and Reducer classes
        job.setMapperClass(GenProgMapper.class);  // Your Mapper class
        job.setReducerClass(GenProgReducer.class);  // Correct Reducer class

        // Set the output types (key, value) for the Reducer
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);

        // Set the input and output paths
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        // Submit the job
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}


