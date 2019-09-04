package org.polarcoordinates.hive.commonprefix;

import java.lang.StringBuilder;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;

import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;

public class CommonPrefix extends GenericUDF {
    
    private StringObjectInspector firstStringInspector;
    private StringObjectInspector secondStringInspector;

    @Override
    public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
        if (arguments.length == 2) {
            if (!(arguments[0] instanceof StringObjectInspector) || !(arguments[1] instanceof StringObjectInspector)) {
                throw new UDFArgumentException(
                        "string compare takes two string arguments");
                    }
            firstStringInspector  = (StringObjectInspector) arguments[0];
            secondStringInspector  = (StringObjectInspector) arguments[1];

        } else {
            throw new UDFArgumentLengthException("FuzzyMatch takes two required arguments [<first string>, <second string>]");
        }

        return PrimitiveObjectInspectorFactory.javaStringObjectInspector;
    }

    @Override
    public Object evaluate(DeferredObject[] arguments) throws HiveException {
        String first = firstStringInspector.getPrimitiveJavaObject(arguments[0].get());
        String second = secondStringInspector.getPrimitiveJavaObject(arguments[1].get());

        StringBuilder result = new StringBuilder();

        int count = 0;
        if (first.length() > second.length()) {
            for (char c : second.toCharArray()) {
                if (c == first.charAt(0)) {
                    result.append(c);
                    ++count;
                } else return result.toString();
            }
        } else {
            for (char c : first.toCharArray()) {
                if (c == second.charAt(0)) {
                    result.append(c);
                    ++count;
                } else return result.toString();
            }
        }

        return result.toString();
    }

    @Override
    public String getDisplayString(String[] children) {
        return "return the common first characters of a string";
    }

}

