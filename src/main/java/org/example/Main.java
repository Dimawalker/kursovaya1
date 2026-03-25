import java.util.concurrent.TimeUnit;

public class Main {
    public static void main(String[] args) throws InterruptedException {
        System.out.println("=== Custom Thread Pool Demo ===\n");

        // Создаем пул
        CustomThreadPool pool = new CustomThreadPool(
                2,     // corePoolSize
                4,     // maxPoolSize
                3,     // keepAliveTime
                TimeUnit.SECONDS,
                2,     // queueSize per thread
                1,     // minSpareThreads
                null   // use default handler
        );

        System.out.println("Pool created: core=2, max=4, queue=2, minSpare=1\n");

        // Тест 1: Нормальная нагрузка
        System.out.println("--- Test 1: 5 tasks ---");
        for (int i = 1; i <= 5; i++) {
            final int id = i;
            pool.execute(() -> {
                System.out.println("  Task " + id + " started");
                try { Thread.sleep(1000); }
                catch (InterruptedException e) { }
                System.out.println("  Task " + id + " finished");
            });
        }

        Thread.sleep(5000);
        System.out.println("Stats: threads=" + pool.getTotalThreadCount() +
                ", active=" + pool.getActiveThreadCount() +
                ", queued=" + pool.getQueuedTaskCount() + "\n");

        // Тест 2: Перегрузка
        System.out.println("--- Test 2: 20 tasks (overload) ---");
        for (int i = 1; i <= 20; i++) {
            final int id = i;
            pool.execute(() -> {
                System.out.println("  Task " + id + " started");
                try { Thread.sleep(2000); }
                catch (InterruptedException e) { }
            });
        }

        Thread.sleep(3000);
        System.out.println("Stats during overload: threads=" + pool.getTotalThreadCount() +
                ", active=" + pool.getActiveThreadCount() +
                ", queued=" + pool.getQueuedTaskCount());

        Thread.sleep(8000);

        // Тест 3: Shutdown
        System.out.println("\n--- Test 3: Shutdown ---");
        pool.shutdown();

        Thread.sleep(2000);
        System.out.println("Final stats: threads=" + pool.getTotalThreadCount());

        System.out.println("\n=== Demo completed ===");
    }
}