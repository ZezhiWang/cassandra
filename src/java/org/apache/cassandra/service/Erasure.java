package org.apache.cassandra.service;

import com.sun.jna.*;
import java.util.*;

public class Erasure {
//    public interface Kishori extends Library {
//        // GoSlice class maps to:
//        // C type struct { void *data; GoInt len; GoInt cap; }
//        class GoSlice extends Structure {
//            public static class ByValue extends GoSlice implements Structure.ByValue {}
//            public Pointer data;
//            public long len;
//            public long cap;
//            protected List getFieldOrder(){
//                return Arrays.asList(new String[]{"data","len","cap"});
//            }
//        }
//
//        // GoString class maps to:
//        // C type struct { const char *p; GoInt n; }
//        class GoString extends Structure {
//            public static class ByValue extends GoString implements Structure.ByValue {}
//            public String p;
//            public long n;
//            protected List getFieldOrder(){
//                return Arrays.asList(new String[]{"p","n"});
//            }
//
//        }
//
//        GoString.ByValue Encode(GoSlice.ByValue bytes);
//        GoString.ByValue Decode(GoString.ByValue encodedString, long p1);
//    }

    public static String decode(List<String> strList) {
        return String.join(";", strList);
    }

    public static String[] encode(String str) {
        return str.split(";");
    }

//    static public void main(String argv[]) {
//
//        //Replace this path with the path on your local machine
//        Kishori erasure = Native.load(
//                "/home/ubuntu/treas/src/golang/erasure.so", Kishori.class);
//        byte[] bytes = "verylongstringdatajuanherrykishoriinput".getBytes();
//
//        Memory arr = new Memory(bytes.length * Native.getNativeSize(Byte.TYPE));
//        arr.write(0, bytes, 0, bytes.length);
//
//        Kishori.GoSlice.ByValue slice = new Kishori.GoSlice.ByValue();
//        slice.data = arr;
//        slice.len = bytes.length;
//        slice.cap = bytes.length;
//
//        Kishori.GoString.ByValue encodedGoString = erasure.Encode(slice);
//        System.out.println(encodedGoString.p);
//
//        Kishori.GoString.ByValue decodedGoString = erasure.Decode(encodedGoString,encodedGoString.n/4);
//        System.out.println(decodedGoString.p);
//    }
}