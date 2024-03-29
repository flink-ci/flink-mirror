/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.checkpoint;

import org.apache.flink.runtime.state.CompositeStateHandle;
import org.apache.flink.runtime.state.StateObject;
import org.apache.flink.runtime.state.StateUtil;

import org.apache.commons.collections.CollectionUtils;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.function.Predicate;
import java.util.stream.Stream;

/**
 * This class represents a generic collection for {@link StateObject}s. Being a state object itself,
 * it delegates {@link #discardState()} to all contained state objects and computes {@link
 * #getStateSize()} as sum of the state sizes of all contained objects.
 *
 * @param <T> type of the contained state objects.
 */
public class StateObjectCollection<T extends StateObject> implements Collection<T>, StateObject {

    private static final long serialVersionUID = 1L;

    /** The empty StateObjectCollection. */
    private static final StateObjectCollection<?> EMPTY =
            new StateObjectCollection<>(Collections.emptyList());

    /** Wrapped collection that contains the state objects. */
    private final Collection<T> stateObjects;

    /** Creates a new StateObjectCollection that is backed by an {@link ArrayList}. */
    public StateObjectCollection() {
        this.stateObjects = new ArrayList<>();
    }

    /**
     * Creates a new StateObjectCollection wraps the given collection and delegates to it.
     *
     * @param stateObjects collection of state objects to wrap.
     */
    public StateObjectCollection(Collection<T> stateObjects) {
        this.stateObjects = stateObjects != null ? stateObjects : Collections.emptyList();
    }

    @Override
    public int size() {
        return stateObjects.size();
    }

    @Override
    public boolean isEmpty() {
        return stateObjects.isEmpty();
    }

    @Override
    public boolean contains(Object o) {
        return stateObjects.contains(o);
    }

    @Override
    public Iterator<T> iterator() {
        return stateObjects.iterator();
    }

    @Override
    public Object[] toArray() {
        return stateObjects.toArray();
    }

    @Override
    public <T1> T1[] toArray(T1[] a) {
        return stateObjects.toArray(a);
    }

    @Override
    public boolean add(T t) {
        return stateObjects.add(t);
    }

    @Override
    public boolean remove(Object o) {
        return stateObjects.remove(o);
    }

    @Override
    public boolean containsAll(Collection<?> c) {
        return stateObjects.containsAll(c);
    }

    @Override
    public boolean addAll(Collection<? extends T> c) {
        return stateObjects.addAll(c);
    }

    @Override
    public boolean removeAll(Collection<?> c) {
        return stateObjects.removeAll(c);
    }

    @Override
    public boolean removeIf(Predicate<? super T> filter) {
        return stateObjects.removeIf(filter);
    }

    @Override
    public boolean retainAll(Collection<?> c) {
        return stateObjects.retainAll(c);
    }

    @Override
    public void clear() {
        stateObjects.clear();
    }

    @Override
    public void discardState() throws Exception {
        StateUtil.bestEffortDiscardAllStateObjects(stateObjects);
    }

    @Override
    public long getStateSize() {
        return streamAllStateObjects().mapToLong(StateObject::getStateSize).sum();
    }

    @Override
    public void collectSizeStats(StateObjectSizeStatsCollector collector) {
        streamAllStateObjects().forEach(object -> object.collectSizeStats(collector));
    }

    public long getCheckpointedSize() {
        return streamAllStateObjects()
                .mapToLong(StateObjectCollection::getCheckpointedSizeNullSafe)
                .sum();
    }

    private Stream<T> streamAllStateObjects() {
        return stateObjects.stream().filter(Objects::nonNull);
    }

    /** Returns true if this contains at least one {@link StateObject}. */
    public boolean hasState() {
        for (StateObject state : stateObjects) {
            if (state != null) {
                return true;
            }
        }
        return false;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        StateObjectCollection<?> that = (StateObjectCollection<?>) o;

        // simple equals can cause troubles here because of how equals works e.g. between lists and
        // sets.
        return CollectionUtils.isEqualCollection(stateObjects, that.stateObjects);
    }

    @Override
    public int hashCode() {
        return stateObjects.hashCode();
    }

    @Override
    public String toString() {
        return "StateObjectCollection{" + stateObjects + '}';
    }

    public List<T> asList() {
        return stateObjects instanceof List
                ? (List<T>) stateObjects
                : stateObjects != null ? new ArrayList<>(stateObjects) : Collections.emptyList();
    }

    // ------------------------------------------------------------------------
    //  Helper methods.
    // ------------------------------------------------------------------------

    @SuppressWarnings("unchecked")
    public static <T extends StateObject> StateObjectCollection<T> empty() {
        return (StateObjectCollection<T>) EMPTY;
    }

    public static <T extends StateObject> StateObjectCollection<T> emptyIfNull(
            @Nullable StateObjectCollection<T> collection) {
        return collection == null ? empty() : collection;
    }

    public static <T extends StateObject> StateObjectCollection<T> singleton(T stateObject) {
        return new StateObjectCollection<>(Collections.singleton(stateObject));
    }

    public static <T extends StateObject> StateObjectCollection<T> singletonOrEmpty(
            @Nullable T stateObject) {
        return stateObject == null ? empty() : singleton(stateObject);
    }

    private static long getSizeNullSafe(StateObject stateObject) {
        return stateObject != null ? stateObject.getStateSize() : 0L;
    }

    private static long getCheckpointedSizeNullSafe(StateObject stateObject) {
        return stateObject instanceof CompositeStateHandle
                ? ((CompositeStateHandle) stateObject).getCheckpointedSize()
                : getSizeNullSafe(stateObject);
    }
}
