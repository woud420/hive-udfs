package org.polarcoordinates.hive.uuidv5;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.polarcoordinates.hive.uuidv5.UUID5;

import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF.DeferredObject;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;
import org.junit.Test;

public class UUID5Test {
	
    @Test (expected = HiveException.class)
    public void emptyNamespace() throws Exception {
        UUID5 uuid5 = new UUID5();

        StringObjectInspector insp1 = mock(StringObjectInspector.class);
        StringObjectInspector insp2 = mock(StringObjectInspector.class);

        ObjectInspector[] inspectors = new ObjectInspector[] {insp1, insp2};
        uuid5.initialize(inspectors);
        
        DeferredObject namespace = mock(DeferredObject.class);
		DeferredObject value = mock(DeferredObject.class);

        when(namespace.get()).thenReturn("");
		when(value.get()).thenReturn("B073FHWTPL");

        DeferredObject[] vals = new DeferredObject[] {namespace, value};

        uuid5.evaluate(vals);
        uuid5.close();
    }

    @Test (expected = HiveException.class)
	public void emptyValue() throws Exception {
	    UUID5 uuid5 = new UUID5();

        StringObjectInspector insp1 = mock(StringObjectInspector.class);
        StringObjectInspector insp2 = mock(StringObjectInspector.class);

        ObjectInspector[] inspectors = new ObjectInspector[] {insp1, insp2};
        uuid5.initialize(inspectors);
        
        DeferredObject namespace = mock(DeferredObject.class);
		DeferredObject value = mock(DeferredObject.class);

        when(namespace.get()).thenReturn("Keepa");
		when(value.get()).thenReturn("");

        DeferredObject[] vals = new DeferredObject[] {namespace, value};

        uuid5.evaluate(vals);
        uuid5.close();
    }

	@Test
	public void createUUID() throws Exception {
	    UUID5 uuid5 = new UUID5();

        StringObjectInspector insp1 = PrimitiveObjectInspectorFactory.javaStringObjectInspector;
        StringObjectInspector insp2 = PrimitiveObjectInspectorFactory.javaStringObjectInspector;

        ObjectInspector[] inspectors = new ObjectInspector[] {insp1, insp2};
        uuid5.initialize(inspectors);
        
        DeferredObject namespace = mock(DeferredObject.class);
		DeferredObject value = mock(DeferredObject.class);

        when(namespace.get()).thenReturn("keepa");
		when(value.get()).thenReturn("B073FHWTPL");

        DeferredObject[] vals = new DeferredObject[] {namespace, value};

        Object uv5 = uuid5.evaluate(vals);

		assertEquals("2e071d92-f119-5700-9190-f0904de83e94", uv5.toString());
    }   
}
