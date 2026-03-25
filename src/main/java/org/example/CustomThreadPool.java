import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;

public class CustomThreadPool implements CustomExecutor {
    // Параметры пула
    private final int corePoolSize;
    private final int maxPoolSize;
    private final long keepAliveTime;
    private final TimeUnit timeUnit;
    private final int queueSize;
    private final int minSpareThreads;

    // Структуры данных
    private final List<BlockingQueue<Runnable>> taskQueues;
    private final List<Worker> workers;
    private final ReentrantLock lock = new ReentrantLock();

    // Состояние
    private volatile boolean isShutdown = false;
    private final AtomicInteger totalThreads = new AtomicInteger(0);
    private final AtomicInteger activeThreads = new AtomicInteger(0);

    // Балансировщик и обработчик отказов
    private final LoadBalancer loadBalancer;
    private final RejectedExecutionHandler rejectedHandler;

    // Интерфейс балансировщика
    private interface LoadBalancer {
        int getNextQueueIndex(List<BlockingQueue<Runnable>> queues);
    }

    // Реализация Least Loaded
    private class LeastLoadedBalancer implements LoadBalancer {
        @Override
        public int getNextQueueIndex(List<BlockingQueue<Runnable>> queues) {
            if (queues.isEmpty()) return 0;

            int minIndex = 0;
            int minSize = queues.get(0).size();

            for (int i = 1; i < queues.size(); i++) {
                int size = queues.get(i).size();
                if (size < minSize) {
                    minSize = size;
                    minIndex = i;
                }
            }
            return minIndex;
        }
    }

    // Собственный обработчик отказов
    private static class CustomRejectedHandler implements RejectedExecutionHandler {
        @Override
        public void rejectedExecution(Runnable r, ThreadPoolExecutor executor) {
            System.out.println("[Rejected] Task " + r.toString() + " will run in caller thread");
            r.run();
        }
    }

    // Конструктор
    public CustomThreadPool(int corePoolSize, int maxPoolSize, long keepAliveTime,
                            TimeUnit timeUnit, int queueSize, int minSpareThreads,
                            RejectedExecutionHandler rejectedHandler) {
        this.corePoolSize = corePoolSize;
        this.maxPoolSize = maxPoolSize;
        this.keepAliveTime = keepAliveTime;
        this.timeUnit = timeUnit;
        this.queueSize = queueSize;
        this.minSpareThreads = minSpareThreads;
        this.rejectedHandler = rejectedHandler != null ? rejectedHandler : new CustomRejectedHandler();

        this.taskQueues = new CopyOnWriteArrayList<>();
        this.workers = new CopyOnWriteArrayList<>();
        this.loadBalancer = new LeastLoadedBalancer();

        // Создаем core потоки
        for (int i = 0; i < corePoolSize; i++) {
            addWorker();
        }
    }

    // Рабочий поток
    private class Worker implements Runnable {
        private final Thread thread;
        private final BlockingQueue<Runnable> taskQueue;
        private volatile boolean isRunning = true;
        private volatile long lastTaskTime;

        public Worker() {
            CustomThreadFactory factory = new CustomThreadFactory("MyPool");
            this.taskQueue = new LinkedBlockingQueue<>(queueSize);
            this.thread = factory.newThread(this);
            this.lastTaskTime = System.currentTimeMillis();

            lock.lock();
            try {
                taskQueues.add(taskQueue);
                workers.add(this);
                totalThreads.incrementAndGet();
            } finally {
                lock.unlock();
            }

            thread.start();
        }

        @Override
        public void run() {
            while (isRunning && !isShutdown) {
                try {
                    Runnable task = null;

                    // Проверяем таймаут простоя
                    long idleTime = System.currentTimeMillis() - lastTaskTime;
                    long keepAliveMillis = timeUnit.toMillis(keepAliveTime);

                    if (totalThreads.get() > corePoolSize && idleTime > keepAliveMillis) {
                        System.out.println("[Worker] " + Thread.currentThread().getName() +
                                " idle timeout, stopping.");
                        break;
                    }

                    // Ждем задачу
                    task = taskQueue.poll(keepAliveTime, timeUnit);

                    if (task != null) {
                        activeThreads.incrementAndGet();
                        lastTaskTime = System.currentTimeMillis();

                        try {
                            System.out.println("[Worker] " + Thread.currentThread().getName() +
                                    " executes " + task.toString());
                            task.run();
                        } catch (Exception e) {
                            System.err.println("[Worker] Error: " + e.getMessage());
                        } finally {
                            activeThreads.decrementAndGet();
                        }
                    }

                    // Проверяем резервные потоки
                    checkAndCreateSpareThreads();

                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }

            // Завершение
            lock.lock();
            try {
                workers.remove(this);
                taskQueues.remove(taskQueue);
                totalThreads.decrementAndGet();
                System.out.println("[Worker] " + Thread.currentThread().getName() + " terminated.");
            } finally {
                lock.unlock();
            }
        }

        public void stop() {
            isRunning = false;
            thread.interrupt();
        }

        public BlockingQueue<Runnable> getTaskQueue() {
            return taskQueue;
        }
    }

    private void checkAndCreateSpareThreads() {
        if (isShutdown) return;

        int freeThreads = totalThreads.get() - activeThreads.get();
        if (freeThreads < minSpareThreads && totalThreads.get() < maxPoolSize) {
            lock.lock();
            try {
                freeThreads = totalThreads.get() - activeThreads.get();
                if (freeThreads < minSpareThreads && totalThreads.get() < maxPoolSize) {
                    int threadsToCreate = Math.min(minSpareThreads - freeThreads,
                            maxPoolSize - totalThreads.get());
                    for (int i = 0; i < threadsToCreate; i++) {
                        addWorker();
                    }
                }
            } finally {
                lock.unlock();
            }
        }
    }

    private void addWorker() {
        if (isShutdown) return;
        new Worker();
    }

    @Override
    public void execute(Runnable command) {
        if (command == null) throw new NullPointerException();
        if (isShutdown) {
            System.out.println("[Rejected] Task rejected - pool is shutdown");
            rejectedHandler.rejectedExecution(command, null);
            return;
        }

        if (taskQueues.isEmpty()) {
            System.out.println("[Rejected] No queues available");
            rejectedHandler.rejectedExecution(command, null);
            return;
        }

        // Выбираем очередь с наименьшей загрузкой
        int queueIndex = loadBalancer.getNextQueueIndex(taskQueues);
        BlockingQueue<Runnable> selectedQueue = taskQueues.get(queueIndex);

        System.out.println("[Pool] Task accepted into queue #" + queueIndex + ": " +
                command.toString());

        boolean offered = selectedQueue.offer(command);

        if (!offered) {
            // Пытаемся создать новый поток
            if (totalThreads.get() < maxPoolSize) {
                lock.lock();
                try {
                    if (totalThreads.get() < maxPoolSize) {
                        addWorker();
                        if (!taskQueues.isEmpty()) {
                            BlockingQueue<Runnable> newQueue = taskQueues.get(taskQueues.size() - 1);
                            offered = newQueue.offer(command);
                        }
                    }
                } finally {
                    lock.unlock();
                }
            }

            if (!offered) {
                System.out.println("[Rejected] Task " + command.toString() +
                        " was rejected due to overload!");
                // Выполняем в текущем потоке
                command.run();
            }
        }
    }

    @Override
    public <T> Future<T> submit(Callable<T> callable) {
        FutureTask<T> futureTask = new FutureTask<>(callable);
        execute(futureTask);
        return futureTask;
    }

    @Override
    public void shutdown() {
        System.out.println("[Pool] Shutdown initiated");
        isShutdown = true;

        for (Worker worker : workers) {
            worker.stop();
        }
    }

    @Override
    public List<Runnable> shutdownNow() {
        System.out.println("[Pool] ShutdownNow initiated");
        isShutdown = true;

        List<Runnable> remainingTasks = new ArrayList<>();

        for (BlockingQueue<Runnable> queue : taskQueues) {
            queue.drainTo(remainingTasks);
        }

        for (Worker worker : workers) {
            worker.stop();
        }

        return remainingTasks;
    }

    // Методы для мониторинга
    public int getActiveThreadCount() {
        return activeThreads.get();
    }

    public int getTotalThreadCount() {
        return totalThreads.get();
    }

    public int getQueuedTaskCount() {
        return taskQueues.stream().mapToInt(BlockingQueue::size).sum();
    }
}