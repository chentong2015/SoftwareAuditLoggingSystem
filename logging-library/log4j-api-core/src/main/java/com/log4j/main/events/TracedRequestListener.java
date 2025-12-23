package com.log4j.main.events;

public interface TracedRequestListener {

    default void onRequestStarted(String propertiesNode) {
    }

    default void onRequestFinished(String propertiesNode) {
    }
}
