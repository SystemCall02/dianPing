package com.hmdp.utils;

public interface ILock {
    boolean tryLock(long timeOutSecond);

    void unlock();
}
