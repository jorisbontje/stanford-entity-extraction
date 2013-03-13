package stanford;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.junit.Before;
import org.junit.Test;

public class StanfordTest {

  MapDriver<LongWritable, Text, Text, Text> mapDriver;

  @Before
  public void setUp() {

    StanfordMapper mapper = new StanfordMapper();
    mapDriver = new MapDriver<LongWritable, Text, Text, Text>();
    mapDriver.setMapper(mapper);
  }

  @Test
  public void testMapper() {
	  mapDriver.withInput(new LongWritable(1), new Text("Good afternoon Rajat Raina, how are you today?"))
	  .withOutput(new Text("Rajat Raina"), new Text("PERSON")).runTest();
  }
}
