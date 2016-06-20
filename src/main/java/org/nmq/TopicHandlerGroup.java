package org.nmq;

import java.util.Collection;
import java.util.Iterator;
import java.util.concurrent.CopyOnWriteArrayList;

import org.nmq.channelhandler.ClientMessageHandler;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class TopicHandlerGroup implements Iterable<ClientMessageHandler> {

    private final String topic;
    private final CopyOnWriteArrayList<ClientMessageHandler> topicHandlers = new CopyOnWriteArrayList<>();

    public TopicHandlerGroup(String topic) {
        this.topic = topic;
    }

    public int size() {
        return topicHandlers.size();
    }

    public boolean isEmpty() {
        return topicHandlers.isEmpty();
    }

    public boolean contains(Object handler) {
        return topicHandlers.contains(handler);
    }

    public Iterator<ClientMessageHandler> iterator() {
        return topicHandlers.iterator();
    }

    public Object[] toArray() {
        return topicHandlers.toArray();
    }

    public <T> T[] toArray(T[] a) {
        return topicHandlers.toArray(a);
    }

    public ClientMessageHandler get(int index) {
        return topicHandlers.get(index);
    }

    public boolean add(ClientMessageHandler handler) {
        boolean added = topicHandlers.add(handler);

        return added;
    }

    public boolean remove(ClientMessageHandler handler) {
        return topicHandlers.remove(handler);

    }

    public boolean containsAll(Collection<?> h) {
        return topicHandlers.containsAll(h);
    }

    public boolean addAll(Collection<? extends ClientMessageHandler> h) {
        return topicHandlers.addAll(h);
    }

    public boolean retainAll(Collection<?> h) {
        return topicHandlers.retainAll(h);
    }

    public boolean removeAll(Collection<?> h) {
        return topicHandlers.removeAll(h);
    }

    public void clear() {
        topicHandlers.clear();
    }

    public String topic() {
        return topic;
    }

}
