package com.example.data.quality.utils;

import scala.collection.JavaConversions;
import scala.collection.JavaConverters;
import scala.collection.Seq;

import java.util.List;
import java.util.Map;

public class JavaToScalaUtils {

    public static <T> Seq<T> toSeq(List<T> list) {
        return JavaConversions.asScalaBuffer(list).toSeq();
    }

    public static <T> Map<T, Object> toJavaMap(scala.collection.immutable.Map<T, Object> map) {
        return JavaConverters.mapAsJavaMapConverter(map).asJava();
    }
}
