package stanford;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;

import edu.stanford.nlp.ie.AbstractSequenceClassifier;
import edu.stanford.nlp.ie.crf.CRFClassifier;
import edu.stanford.nlp.util.Triple;

public class NERUDF extends GenericUDTF {

	private Object[] forwardObj = null;
	private PrimitiveObjectInspector stringOI;
	private AbstractSequenceClassifier classifier;

	
	@Override
	public StructObjectInspector initialize(ObjectInspector[] args)
			throws UDFArgumentException {
		
		if (args.length != 2) {
		throw new UDFArgumentLengthException(
					"The operator 'NERUDF' accepts 2 arguments.");
		}
		
		String serializedClassifier = "/tmp/english.all.3class.distsim.crf.ser.gz";
		this.classifier = CRFClassifier.getClassifierNoExceptions(serializedClassifier);
		
		stringOI = (PrimitiveObjectInspector) args[0];
		
		ArrayList<String> fieldNames = new ArrayList<String>();
		ArrayList<ObjectInspector> fieldOIs = new ArrayList<ObjectInspector>();

		fieldNames.add("entityName");
		fieldOIs.add(PrimitiveObjectInspectorFactory
				.getPrimitiveJavaObjectInspector(PrimitiveCategory.STRING));

		fieldNames.add("entityType");
		fieldOIs.add(PrimitiveObjectInspectorFactory
				.getPrimitiveJavaObjectInspector(PrimitiveCategory.STRING));

		fieldNames.add("messageId");
		fieldOIs.add(PrimitiveObjectInspectorFactory
				.getPrimitiveJavaObjectInspector(PrimitiveCategory.STRING));

		this.forwardObj = new Object[3];
		return ObjectInspectorFactory.getStandardStructObjectInspector(
				fieldNames, fieldOIs);
	}

	@Override
	public void process(Object[] args) throws HiveException, UDFArgumentException {
		String messageId = (String) stringOI.getPrimitiveJavaObject(args[0]);
		String body = (String) stringOI.getPrimitiveJavaObject(args[1]);

		List<Triple<String, Integer, Integer>> result = classifier
				.classifyToCharacterOffsets(body);
		
		for (Triple<String, Integer, Integer> triple : result) {
			this.forwardObj[0] = body.toString().substring(triple.second, triple.third);
			this.forwardObj[1] = triple.first;
			this.forwardObj[2] = messageId;
			forward(forwardObj);
		}
	}

	@Override
	public void close() throws HiveException {
	}
}
