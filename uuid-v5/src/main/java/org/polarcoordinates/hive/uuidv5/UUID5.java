package org.polarcoordinates.hive.uuidv5;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;

import java.util.UUID;

public class UUID5 extends GenericUDF {

    private StringObjectInspector firstStringInspector;
    private StringObjectInspector secondStringInspector;

    private final UUID KEEPA_NAMESPACE = UUID.fromString("48c457f7-d1b6-4e0f-a67c-7a0ab1af79c0");

    @Override
    public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
        if (arguments.length == 2) {
            if (!(arguments[0] instanceof StringObjectInspector) || !(arguments[1] instanceof StringObjectInspector)) {
                throw new UDFArgumentException(
                        "all required arguments must be strings");
                    }
            firstStringInspector  = (StringObjectInspector) arguments[0];
            secondStringInspector  = (StringObjectInspector) arguments[1];

        } else {
            throw new UDFArgumentLengthException("UUID5 takes 2 required arguments [<namespace string>, <value string>]");
        }

        return PrimitiveObjectInspectorFactory.javaFloatObjectInspector;
    }

    @Override
    public Object evaluate(GenericUDF.DeferredObject[] arguments) throws HiveException {
        
        UUID namespaceUUID;
        String namespace = firstStringInspector.getPrimitiveJavaObject(arguments[0].get());
        String value = secondStringInspector.getPrimitiveJavaObject(arguments[1].get());

        if (value == null || value.length() == 0) {
            throw new HiveException("UUID5 cannot encode empty value string");
        }
        if (namespace == null || namespace.length() == 0) {
            throw new HiveException("UUID5 cannot encode empty namespace string");
        }

        if (namespace.toLowerCase().equals("keepa")) {
            namespaceUUID = KEEPA_NAMESPACE;
        } else {
            try {
                namespaceUUID = UUID.fromString(namespace);
            }
            catch(Exception e) {
                throw new HiveException("error parsing namespace uuid", e);
            }
        }

        return UUID5_Helper.fromUTF8(namespaceUUID, value);
    }

    @Override
    public String getDisplayString(String[] children) {
        return "encode string value using UUID type 5";
    }

}

