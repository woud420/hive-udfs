package org.polarcoordinates.hive.fuzzymatch;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;

import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;

import org.apache.lucene.search.spell.JaroWinklerDistance;
import org.apache.lucene.search.spell.LevenshteinDistance;
import org.apache.lucene.search.spell.LuceneLevenshteinDistance;
import org.apache.lucene.search.spell.NGramDistance;
import org.apache.lucene.search.spell.StringDistance;

public class FuzzyMatch extends GenericUDF {

    private static final String JARO_WINCKLER = "jw";
    private static final String NGRAM = "ng";
    private static final String LEVENSTEIN = "lv";
    private static final String LUCENE_LEVENSTEIN = "llv";

    private static final StringDistance JW_DIST = new JaroWinklerDistance();
    private static final StringDistance NG_DIST = new NGramDistance();
    private static final StringDistance LV_DIST = new LevenshteinDistance();
    private static final StringDistance LLV_DIST = new LuceneLevenshteinDistance();

    private StringObjectInspector firstStringInspector;
    private StringObjectInspector secondStringInspector;
    private StringObjectInspector algoInspector;

    @Override
    public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
        ObjectInspector first = arguments[0];
        ObjectInspector second = arguments[1];
        ObjectInspector algo = arguments[2];

        if (arguments.length == 3) {
            first = arguments[0];
            second = arguments[1];
            algo = arguments[2];

            if (!(first instanceof StringObjectInspector) || !(second instanceof StringObjectInspector) 
                    || !(algo instanceof StringObjectInspector)) {
                throw new UDFArgumentException(
                        "all required arguments must be strings - algo in third position, available : jw, ng, lv, or llv");
                    }

        } else {
            throw new UDFArgumentLengthException("FuzzyMatch takes 3 required arguments [<first string>, <second string>, <scoring algo>]");
        }

        return PrimitiveObjectInspectorFactory.javaFloatObjectInspector;
    }

    @Override
    public Object evaluate(DeferredObject[] arguments) throws HiveException {
        String first = firstStringInspector.getPrimitiveJavaObject(arguments[0].get());
        String second = secondStringInspector.getPrimitiveJavaObject(arguments[1].get());
        String algo = algoInspector.getPrimitiveJavaObject(arguments[2].get()).toLowerCase();

        if (algo == null || algo.length() == 0) {
            throw new HiveException(
                    "The Algorithmic Distance Scorer may not be empty. \n - available algos are jw (jaro-winkler w/ threshold of 0.7), ng (n-gram of size 2), lv (levenstein) or llv(lucene levenshtein / damarau-levenshtein");
        }

        // No Matter the algo, if they're both empty or null strings, distance will be 1
        if (first == null || first.length() == 0) {
            if (second == null || second.length() == 0)
                return 1;
            else
                return 0;
        }

        switch (algo) {
            case JARO_WINCKLER:
                return JW_DIST.getDistance(first, second);
            case NGRAM:
                return NG_DIST.getDistance(first, second);
            case LEVENSTEIN:
                return LV_DIST.getDistance(first, second);
            case LUCENE_LEVENSTEIN:
                return LLV_DIST.getDistance(first, second);
            default:
                throw new HiveException(algo + " is not a valid algorithm.");
        }
    }

    @Override
    public String getDisplayString(String[] children) {
        return "evaluate fuzzy match score based distance of two strings";
    }

}

