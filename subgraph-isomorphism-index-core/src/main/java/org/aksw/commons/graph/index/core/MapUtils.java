package org.aksw.commons.graph.index.core;

import java.util.Map;
import java.util.Objects;
import java.util.Set;

import com.google.common.collect.Sets;

public class MapUtils {
    public static boolean isCompatible(Map<?, ?> a, Map<?, ?> b) {
        Set<?> commonKeys = Sets.intersection(a.keySet(), b.keySet());
        boolean result = isCompatible(commonKeys, a, b);
        return result;
    }

    public static <K, V> boolean isCompatible(Set<?> keysToTest, Map<?, ?> a, Map<?, ?> b) {
// TODO We could use a parallel stream based version
//        boolean result = keysToTest.stream().allMatch(key -> {
//            V av = a.get(key);
//            V bv = b.get(key);
//            boolean r = Objects.equal(av, bv);
//            return r;
//        });
    	
    	
    	boolean result = true;
        for(Object key : keysToTest) {
            Object av = a.get(key);
            Object bv = b.get(key);
            result = Objects.equals(av, bv);
            if(!result) {
                break;
            }
        }

        return result;
    }
}
