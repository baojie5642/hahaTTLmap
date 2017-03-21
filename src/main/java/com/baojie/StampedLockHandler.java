package com.baojie;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.StampedLock;

public class StampedLockHandler {

	private static final int LOCK_WAIT_TIME = 60;//默认单位为秒

	private StampedLockHandler() {

	}

	public static long getReadLock(final StampedLock stampedLock) {
		if(null==stampedLock){
			throw new NullPointerException();
		}
		long stamp = 0L;
		try {
			stamp = stampedLock.tryReadLock(LOCK_WAIT_TIME, TimeUnit.SECONDS);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		return stamp;
	}

	public static long getWriteLock(final StampedLock stampedLock) {
		if(null==stampedLock){
			throw new NullPointerException();
		}
		long stamp = 0L;
		try {
			stamp = stampedLock.tryWriteLock(LOCK_WAIT_TIME, TimeUnit.SECONDS);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		return stamp;
	}

	public static long getOptimisticRead(final StampedLock stampedLock) {
		if(null==stampedLock){
			throw new NullPointerException();
		}
		final long stamp = stampedLock.tryOptimisticRead();
		return stamp;
	}
}
