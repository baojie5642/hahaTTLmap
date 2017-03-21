package com.baojie;

import static java.util.concurrent.TimeUnit.NANOSECONDS;

import java.io.Serializable;
import java.util.concurrent.Delayed;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

public class TTLStamp<T> implements Delayed, Serializable {

	private static final long serialVersionUID = -2017031516252955555L;
	private static final AtomicLong sequencer = new AtomicLong();
	private final TimeUnit delayTimeUnit;
	private final Long sequenceNumber;
	private final Long delayTime;
	private final Long takeTime;
	private final T hahaEntity;
	private final String key;

	public TTLStamp(final T hahaEntity, final long delayTime, final TimeUnit delayTimeUnit, final String key) {
		innerCheck(hahaEntity, delayTime, delayTimeUnit, key);
		this.takeTime = createTakeTime(delayTime, delayTimeUnit);
		this.sequenceNumber = sequencer.getAndIncrement();
		this.delayTimeUnit = delayTimeUnit;
		this.hahaEntity = hahaEntity;
		this.delayTime = delayTime;
		this.key = key;
	}

	private void innerCheck(final T hahaEntity, final long delayTime, final TimeUnit delayTimeUnit, final String key) {
		if (null == hahaEntity) {
			throw new NullPointerException("'hahaEntity' must not be null");
		}
		if (null == key) {
			throw new NullPointerException("'key' must not be null");
		}
		if (key.length() == 0) {
			throw new IllegalStateException("'key' must not be empty");
		}
		if (delayTime <= 0L) {
			throw new IllegalStateException("'delayTime' <= 0");
		}
		if (null == delayTimeUnit) {
			throw new NullPointerException("'delayTimeUnit' must not be null");
		}
	}

	private long createTakeTime(final long delayTime, final TimeUnit delayTimeUnit) {
		return TimeUnit.NANOSECONDS.convert(delayTime, delayTimeUnit) + System.nanoTime();
	}

	@Override
	public long getDelay(final TimeUnit unit) {
		return unit.convert(takeTime - now(), NANOSECONDS);
	}

	private final long now() {
		return System.nanoTime();
	}

	@Override
	public int compareTo(final Delayed other) {
		if (null == other) {
			return 1;
		}
		if (other == this) {
			return 0;
		}
		if (other instanceof TTLStamp) {
			@SuppressWarnings("rawtypes")
			TTLStamp x = (TTLStamp) other;
			long diff = takeTime - x.takeTime;
			if (diff < 0)
				return -1;
			else if (diff > 0)
				return 1;
			else if (sequenceNumber < x.sequenceNumber)
				return -1;
			else
				return 1;
		}
		final long diff = getDelay(NANOSECONDS) - other.getDelay(NANOSECONDS);
		return (diff < 0) ? -1 : (diff > 0) ? 1 : 0;
	}

	public long getSequenceNumber() {
		return sequenceNumber;
	}

	public T getHahaEntity() {
		return hahaEntity;
	}

	public long getDelayTime() {
		return delayTime;
	}

	public long getTakeTime() {
		return takeTime;
	}

	public TimeUnit getDelayTimeUnit() {
		return delayTimeUnit;
	}

	public String getKey() {
		return key;
	}

	public boolean isTTL() {
		final long remain = takeTime - System.nanoTime();
		if (remain < 0) {
			return true;
		} else {
			return false;
		}
	}

	@Override
	public String toString() {
		return "TTLStamp{key:" + key + ", delayTime:" + delayTime + ", delayTimeUnit:" + delayTimeUnit.toString()
				+ ", takeTime:" + takeTime + ", sequenceNumber:" + sequenceNumber + "}";
	}

}
