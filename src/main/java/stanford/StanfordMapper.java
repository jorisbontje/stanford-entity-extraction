package stanford;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import edu.stanford.nlp.ie.AbstractSequenceClassifier;
import edu.stanford.nlp.ie.crf.CRFClassifier;
import edu.stanford.nlp.util.Triple;

public class StanfordMapper extends
		Mapper<LongWritable, Text, Text, Text> {

	private AbstractSequenceClassifier classifier;
	private String serializedClassifier;

	@Override
	public void setup(Context context) throws IOException {
		serializedClassifier = context.getConfiguration().get(
				"stanford.classifier",
				"/home/training/stanford-ner-2012-11-11/classifiers/english.all.3class.distsim.crf.ser.gz");
		this.classifier = CRFClassifier.getClassifierNoExceptions(serializedClassifier);
	}

	@Override
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {

		List<Triple<String, Integer, Integer>> result = classifier
				.classifyToCharacterOffsets(value.toString());

		for (Triple<String, Integer, Integer> triple : result) {
			context.write(new Text(value.toString().substring(triple.second, triple.third)),
					new Text(triple.first));
		}
	}
}
