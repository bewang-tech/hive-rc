package org.apache.hive.udfs;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF.DeferredObject;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;

@Description(name = "udf2_func")
public class MyUdf2 extends GenericUDF {

	public String getDisplayString(String[] args) {
		return "udf2_func";
	}

	public ObjectInspector initialize(ObjectInspector[] arguments)
			throws UDFArgumentException {
		return null;
	}

	public Object evaluate(DeferredObject[] arguments) throws HiveException {
		return null;
	}

}
