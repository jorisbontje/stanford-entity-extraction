package stanford;

import java.util.ArrayList;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.WritableConstantIntObjectInspector;
import org.apache.hadoop.io.IntWritable;

public class NERUDF extends GenericUDTF {
	IntWritable start;
	IntWritable end;
	IntWritable inc;
	Object[] forwardObj = null;

	@Override
	public StructObjectInspector initialize(ObjectInspector[] args)
			throws UDFArgumentException {
		start = ((WritableConstantIntObjectInspector) args[0])
				.getWritableConstantValue();
		end = ((WritableConstantIntObjectInspector) args[1])
				.getWritableConstantValue();
		if (args.length == 3) {
			inc = ((WritableConstantIntObjectInspector) args[2])
					.getWritableConstantValue();
		} else {
			inc = new IntWritable(1);
		}
		this.forwardObj = new Object[1];
		ArrayList<String> fieldNames = new ArrayList<String>();
		ArrayList<ObjectInspector> fieldOIs = new ArrayList<ObjectInspector>();
		fieldNames.add("col0");
		fieldOIs.add(PrimitiveObjectInspectorFactory
				.getPrimitiveJavaObjectInspector(PrimitiveCategory.INT));
		return ObjectInspectorFactory.getStandardStructObjectInspector(
				fieldNames, fieldOIs);
	}

	@Override
	public void process(Object[] args) throws HiveException,
			UDFArgumentException {
		for (int i = start.get(); i < end.get(); i = i + inc.get()) {
			this.forwardObj[0] = new Integer(i);
			forward(forwardObj);
		}
	}

	@Override
	public void close() throws HiveException {
	}
}
