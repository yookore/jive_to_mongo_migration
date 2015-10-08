package com.yookos.profile.migration;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingDeque;

public class LaunchPublishers {
	
	public static final int SIZE = 1121;
	public static final int BATCH = 10;
    static BlockingQueue<Integer> queue = new LinkedBlockingDeque<Integer>();

	public static void main(String[] args) {

      	ExecutorService produce = Executors.newSingleThreadExecutor();
      	produce.submit(new Publisher(1, BATCH));
	}
}
