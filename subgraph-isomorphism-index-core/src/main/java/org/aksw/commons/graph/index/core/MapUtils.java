package org.aksw.commons.graph.index.core;

import java.util.Map;
import java.util.Set;

import com.google.common.base.Objects;
import com.google.common.collect.Sets;

public class MapUtils {
    public static <K, V> boolean isCompatible(Map<K, V> a, Map<K, V> b) {
        Set<K> commonKeys = Sets.intersection(a.keySet(), b.keySet());
        boolean result = isCompatible(commonKeys, a, b);
        return result;
    }

    public static <K, V> boolean isCompatible(Set<K> keysToTest, Map<K, V> a, Map<K, V> b) {
        boolean result = true;
        for(K key : keysToTest) {
            V av = a.get(key);
            V bv = b.get(key);
            result = Objects.equal(av, bv);
            if(!result) {
                break;
            }
        }

        return result;
    }
}
