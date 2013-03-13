package stanford;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class StanfordDriver {

  public static void main(String[] args) throws Exception {

    if (args.length != 2) {
      System.out.printf("Usage: StanfordDriver <input dir> <output dir>\n");
      System.exit(-1);
    }

    Job job = new Job();

    job.setJarByClass(StanfordDriver.class);
    job.setJobName("Stanford Driver");

    FileInputFormat.setInputPaths(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));

    job.setMapperClass(StanfordMapper.class);
    job.setNumReduceTasks(0);

    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);

    boolean success = job.waitForCompletion(true);
    System.exit(success ? 0 : 1);
  }
}

