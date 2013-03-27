package stanford;

import java.util.ArrayList;

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

public class NERUDF extends GenericUDTF {

	Object[] forwardObj = null;
	PrimitiveObjectInspector stringOI;
	
	@Override
	public StructObjectInspector initialize(ObjectInspector[] args)
			throws UDFArgumentException {
		
		if (args.length != 2) {
		throw new UDFArgumentLengthException(
					"The operator 'NERUDF' accepts 2 arguments.");
		}
		
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

		for (int i = 0; i < 3; i++) {
			this.forwardObj[0] = new String("entityName" + i);
			this.forwardObj[1] = new String("entityType" + i);
			this.forwardObj[2] = messageId;
			forward(forwardObj);
		}
	}

	@Override
	public void close() throws HiveException {
	}
}
