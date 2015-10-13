package com.yookos.profile.migration;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingDeque;

public class LaunchPublishers {
	
	public static final int SIZE = 1_000;//2_124_155;
	public static final int BATCH = 10;

	public static void main(String[] args) {

      	// create queue
        BlockingQueue<Integer> offset = new LinkedBlockingDeque<Integer>();

        //populate queue
        for (int i = 1; i < SIZE; i += BATCH) {
        	offset.add(i);
        }
        
        int count = 1;
      	ExecutorService produce = Executors.newFixedThreadPool(10);
        while (!offset.isEmpty()) {
        	produce.submit(new Publisher(offset, BATCH, count));
        	count++;
        }
	}
}
