package com.hmdp.utils;

/**
 * @Author xiaohu
 * @Date 2024/11/12 9:17
 * @PackageName:com.hmdp.utils
 * @ClassName: ILock
 * @Description: 分布式锁接口
 * @Version 1.0
 */
public interface ILock {
    boolean tryLock(long timeoutSec);
    void unlock();
    void unlockByLua();
}
