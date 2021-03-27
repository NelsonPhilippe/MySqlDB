package fr.xilitra.mysqldb;

import java.util.concurrent.*;

public abstract class Scheduler implements Runnable{

    private static ScheduledExecutorService scheduledExecutor = Executors.newScheduledThreadPool(Runtime.getRuntime().availableProcessors());
    private static ExecutorService executor = Executors.newCachedThreadPool();

    public static void runTask(Runnable run) {
        executor.submit(run);
    }

    public static void cancelTasks() {
        executor.shutdown();
        scheduledExecutor.shutdown();
    }

    public ScheduledFuture scheduleAsyncDelayedTask(long delay, TimeUnit timeUnit) {
        return scheduledExecutor.schedule(this, delay, timeUnit);
    }

    public ScheduledFuture scheduleAsyncRepeatingTask(long start, long period, TimeUnit timeUnit) {
        return scheduledExecutor.scheduleAtFixedRate(this, start, period, timeUnit);
    }

    public void runTask() {
        Scheduler.runTask(this);
    }

}
