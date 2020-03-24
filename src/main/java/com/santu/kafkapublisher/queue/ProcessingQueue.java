package com.santu.kafkapublisher.queue;

import java.util.LinkedList;
import java.util.Queue;

public class ProcessingQueue {

    private static Queue<String> queue = new LinkedList<>();

    public static Queue<String> getInstance(){
        return queue;
    }

}
