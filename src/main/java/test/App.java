package test;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.gautelis.muprocessmanager.*;
import org.gautelis.muprocessmanager.queue.WorkQueue;
import org.gautelis.muprocessmanager.queue.WorkerQueueFactory;

import java.util.*;

/**
 */
public class App 
{
    private static final Logger log = LogManager.getLogger(App.class);

    public static void main( String... args )
    {
        try {
            final MuProcessManager mngr = MuProcessManager.getManager();
            mngr.start();

            WorkQueue workQueue = WorkerQueueFactory.getWorkQueue(
                    WorkerQueueFactory.Type.Multi,
                    /* number of cores on my machine */ 8
            );
            workQueue.start();

            final Collection<String> sampledCorrelationIds = new LinkedList<>();

            for (int i = 0; i < 100000; i++) {
                final int[] j = {i};

                workQueue.execute(() -> {
                    String correlationId = UUID.randomUUID().toString();
                    if (j[0] % 1000 == 0) {
                        // Sample each thousandth correlation ID
                        sampledCorrelationIds.add(correlationId);
                    }

                    MuProcess process = null;
                    try {
                        process = mngr.newProcess(correlationId);

                        MuProcessResult result = new MuProcessResult("This is a result");

                        MuActivityParameters parameters = new MuActivityParameters();
                        parameters.put("arg1", "param1");
                        process.execute(new FirstActivity(), parameters);

                        parameters.put("arg2", 42);
                        process.execute(
                                (p) -> result.add(10 * (int) p.get("arg2")),
                                new SecondActivityCompensation(),
                                parameters
                        );

                        parameters.put("arg3", true);
                        process.execute(new ThirdActivity(), parameters);

                        parameters.put("arg4", 22 / 7.0);
                        process.execute(new FourthActivity(), parameters);

                        result.add("This is another part of the result");
                        process.finished(result);

                    } catch (MuProcessBackwardBehaviourException mpbae) {
                        // Forward activity failed and so did some compensation activities
                        String info = "Process and compensation failure: " + mpbae.getMessage();
                        if (log.isTraceEnabled()) {
                            log.trace(info);
                        }
                    } catch (MuProcessForwardBehaviourException mpfae) {
                        // Forward activity failed, but compensations were successful
                        String info = "No success, but managed to compensate: " + mpfae.getMessage();
                        if (log.isTraceEnabled()) {
                            log.trace(info);
                        }
                    } catch (Throwable t) {
                        // Other reasons for failure not necessarily related to the activity
                        if (null != process) {
                            process.failed();
                        }

                        String info = "Process failure: " + t.getMessage();
                        log.warn(info, t);
                    }
                });
            }

            do {
                System.out.println("\nProcess result samples:");
                try {
                    // Iterate since we will modify collection
                    Iterator<String> sit = sampledCorrelationIds.iterator();
                    while (sit.hasNext()) {
                        String correlationId = sit.next();

                        System.out.print("correlationId=" + correlationId);
                        Optional<MuProcessStatus> _status = mngr.getProcessStatus(correlationId);
                        if (_status.isPresent()) {
                            MuProcessStatus status = _status.get();
                            System.out.print(" status=" + status);

                            switch (status) {
                                case SUCCESSFUL:
                                    Optional<MuProcessResult> _result = mngr.getProcessResult(correlationId);
                                    _result.ifPresent(objects -> objects.forEach((v) -> System.out.print(" {" + v + "}")));
                                    sit.remove();
                                    break;

                                case NEW:
                                case PROGRESSING:
                                    // Check later
                                    break;

                                default:
                                    // No idea to recheck
                                    sit.remove();
                                    break;
                            }
                        }
                        else {
                            System.out.print(" (running transaction, status not yet visible) ");
                        }
                        System.out.println();
                    }

                    Thread.sleep(20 * 1000); // 20 seconds
                } catch (InterruptedException | MuProcessException ignore) {}
            } while (!workQueue.isEmpty());

            workQueue.stop();

            // Wait a bit more before stopping the manager
            try {
                Thread.sleep(5 * 60 * 1000); // 5 minutes
            }
            catch (InterruptedException ignore) {}

            mngr.stop();
        }
        catch (MuProcessException mpe) {
            String info = "Process manager failure: ";
            info += mpe;
            log.warn(info);
        }
    }
}
