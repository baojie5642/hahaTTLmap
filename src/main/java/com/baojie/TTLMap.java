package com.baojie;

import java.util.ArrayList;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.DelayQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.LockSupport;
import java.util.concurrent.locks.StampedLock;

public class TTLMap {

	private static final int Work_Num = 2;// 尽量小，1也是可以的，越少越好
	private final ThreadPoolExecutor threadPoolExecutor = (ThreadPoolExecutor) Executors
			.newFixedThreadPool(Work_Num * 2);
	private final ConcurrentLinkedQueue<TTLStamp<?>> removeQueue = new ConcurrentLinkedQueue<>();
	private final ConcurrentHashMap<String, TTLStamp<?>> objMap = new ConcurrentHashMap<>(99999);
	private final AtomicBoolean removeRunner = new AtomicBoolean(true);
	private final AtomicBoolean delayRunner = new AtomicBoolean(true);
	private final DelayQueue<TTLStamp<?>> stamps = new DelayQueue<>();
	private final AtomicBoolean mapIsClose = new AtomicBoolean(false);
	private final ArrayList<Future<?>> futures = new ArrayList<>();
	private final StampedLock stampedLock = new StampedLock();

	private TTLMap() {
		init();
	}

	private void init() {
		initDelayRunner();
		initRemoveRunner();
	}

	private void initDelayRunner() {
		GetDelayRunner getDelayRunner = null;
		Future<?> future = null;
		for (int i = 0; i < Work_Num; i++) {
			getDelayRunner = new GetDelayRunner(stampedLock, delayRunner, stamps, removeQueue);
			future = threadPoolExecutor.submit(getDelayRunner);
			futures.add(future);
		}
	}

	private void initRemoveRunner() {
		RemoveRunner removeRunner = null;
		Future<?> future = null;
		for (int i = 0; i < Work_Num; i++) {
			removeRunner = new RemoveRunner(removeQueue, objMap, stampedLock);
			future = threadPoolExecutor.submit(removeRunner);
			futures.add(future);
		}
	}

	public static TTLMap create() {
		return new TTLMap();
	}

	// 不使用了记得关闭，虽然map已经设置了大小99999
	public void closeTTLMap() {
		final boolean state = mapIsClose.get();
		if (true == state) {
			return;
		} else {
			final boolean iAmOnlyThread = mapIsClose.compareAndSet(false, true);
			if (true == iAmOnlyThread) {
				delayRunner.set(false);
				removeRunner.set(false);
				closeFuture();
				cleanMap();
				shuntDownPool();
			} else {
				return;
			}
		}
	}

	private void closeFuture() {
		Future<?> future = null;
		final int futureSize = futures.size();
		for (int i = 0; i < futureSize; i++) {
			future = futures.get(0);
			if (null != future) {
				future.cancel(true);
			}
		}
		futures.clear();
	}

	private void cleanMap() {
		stamps.clear();
		objMap.clear();
	}

	private void shuntDownPool() {
		if (null != threadPoolExecutor) {
			threadPoolExecutor.shutdownNow();
		}
	}

	public boolean isMapClose() {
		return mapIsClose.get();
	}

	public void put(final String key, final Object object, final Long ttl, final TimeUnit timeUnit) {
		if (mapIsClose.get()) {
			return;
		} else {
			final TTLStamp<Object> ttlStamp = new TTLStamp<Object>(object, ttl, timeUnit, key);
			objMap.put(key, ttlStamp);// 先放到map中，并发确保很快的被发现，但是两条语句之间不是原子的，对ttl有影响
			stamps.put(ttlStamp);// 但是如果同步，会导致其他地方也要同步，会导致性能降低，而且这种超时的缓存本来性能就不高
		}
	}

	// 方法允许返回null，自己做好判断
	public TTLStamp<?> get(final String key) {
		if (mapIsClose.get()) {
			return null;
		} else {
			final TTLStamp<?> ttlStamp = objMap.get(key);
			if (null != ttlStamp) {
				objMap.remove(key);
			}
			return ttlStamp;
		}
	}

	private final class GetDelayRunner implements Runnable {

		private final AtomicBoolean delay;
		private final DelayQueue<TTLStamp<?>> delayStamps;
		private final ConcurrentLinkedQueue<TTLStamp<?>> tempQueue;
		private final StampedLock stampedLock;// 后期改进

		public GetDelayRunner(final StampedLock stampedLock, final AtomicBoolean delay,
				final DelayQueue<TTLStamp<?>> delayStamps, final ConcurrentLinkedQueue<TTLStamp<?>> removeQueue) {
			this.delay = delay;
			this.delayStamps = delayStamps;
			this.tempQueue = removeQueue;
			this.stampedLock = stampedLock;
		}

		@Override
		public void run() {
			TTLStamp<?> ttlStamp = null;
			while (delay.get()) {
				ttlStamp = getDelayTTL();
				if (null == ttlStamp) {
					continue;
				} else {
					putTTLIntoQueue(ttlStamp);
				}
			}
			if (delay.get() == false) {
				tempQueue.clear();
			}
		}

		private TTLStamp<?> getDelayTTL() {
			TTLStamp<?> ttlStamp = null;
			try {
				ttlStamp = delayStamps.take();
			} catch (Exception e) {
				ttlStamp = null;
				e.printStackTrace();
			} catch (Throwable throwable) {
				ttlStamp = null;
				throwable.printStackTrace();
			}
			return ttlStamp;
		}

		private void putTTLIntoQueue(final TTLStamp<?> ttlStamp) {
			try {
				tempQueue.offer(ttlStamp);
			} catch (Exception e) {
				e.printStackTrace();
			} catch (Throwable throwable) {
				throwable.printStackTrace();
			}
		}

	}

	private final class RemoveRunner implements Runnable {

		private static final int innerSleepTime = 3;// 针对数据的超时清理，会有短暂的延迟
		private final TimeUnit sleepUnit = TimeUnit.MICROSECONDS;
		private final ConcurrentLinkedQueue<TTLStamp<?>> queue;
		private final ConcurrentHashMap<String, TTLStamp<?>> map;
		private final StampedLock stampedLock;// 后期改进

		public RemoveRunner(final ConcurrentLinkedQueue<TTLStamp<?>> queue,
				final ConcurrentHashMap<String, TTLStamp<?>> map, final StampedLock stampedLock) {
			this.queue = queue;
			this.map = map;
			this.stampedLock = stampedLock;
		}

		@Override
		public void run() {
			Object object = null;
			TTLStamp<?> ttlStamp = null;
			String key = null;
			while (removeRunner.get()) {
				object = getObject();
				if (null == object) {
					Thread.yield();
					sleepDown();
				} else {
					ttlStamp = changeClass(object);
					if (null != ttlStamp) {
						key = ttlStamp.getKey();
						if (null != key) {
							removeKeyObject(key);
						} else {
							Thread.yield();
							sleepDown();
						}
					} else {
						Thread.yield();
						sleepDown();
					}
				}
			}
			if (removeRunner.get() == false) {
				queue.clear();
			}
		}

		private Object getObject() {
			Object object = null;
			try {
				object = queue.poll();
			} catch (Throwable throwable) {
				object = null;
				throwable.printStackTrace();
			}
			return object;
		}

		private void sleepDown() {
			LockSupport.parkNanos(TimeUnit.NANOSECONDS.convert(innerSleepTime, sleepUnit));
		}

		private TTLStamp<?> changeClass(final Object object) {
			TTLStamp<?> ttlStamp = null;
			if (object instanceof TTLStamp<?>) {
				try {
					ttlStamp = (TTLStamp<?>) object;
				} catch (Throwable throwable) {
					ttlStamp = null;
					throwable.printStackTrace();
				}
			} else {
				ttlStamp = null;
			}
			return ttlStamp;
		}

		private void removeKeyObject(final String key) {
			try {
				map.remove(key);
			} catch (Throwable throwable) {
				throwable.printStackTrace();
			}
		}
	}

	public static void main(String args[]) {
		final TTLMap ttlMap = TTLMap.create();
		final AtomicReference<String> key = new AtomicReference<String>(null);
		final Thread thread0 = new Thread(new Runnable() {
			@Override
			public void run() {
				Object object = null;
				for (int i = 0; i < 99999999; i++) {
					Thread.yield();
					LockSupport.parkNanos(TimeUnit.NANOSECONDS.convert(10, TimeUnit.MILLISECONDS));
					object = new Object();
					key.set("" + i);
					ttlMap.put(key.get(), object, 3000L, TimeUnit.MILLISECONDS);
				}
			}
		}, "put");
		thread0.start();
		Thread thread1 = new Thread(new Runnable() {
			@SuppressWarnings("unchecked")
			@Override
			public void run() {
				TTLStamp<Object> ttlStamp = null;
				String keyInner = null;
				for (;;) {
					Thread.yield();
					LockSupport.parkNanos(TimeUnit.NANOSECONDS.convert(10, TimeUnit.MILLISECONDS));
					keyInner = key.get();
					if (null != keyInner) {
						ttlStamp = (TTLStamp<Object>) ttlMap.get(key.get());
						if (null == ttlStamp) {
							Thread.yield();
						} else {
							System.out.println(ttlStamp);
						}
					} else {
						continue;
					}
				}
			}
		}, "take");
		thread1.start();
	}

}
