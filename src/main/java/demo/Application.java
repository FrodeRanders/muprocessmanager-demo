package demo;

import org.gautelis.muprocessmanager.*;
import org.gautelis.vopn.queue.WorkQueue;
import org.gautelis.vopn.queue.WorkerQueueFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 */
public class Application
{
    private static final Logger log = LoggerFactory.getLogger(Application.class);

    private static final Object lock = new Object();

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

            final Collection<String> sampledCorrelationIds = new ArrayList<>();

            for (int i = 0; i < 1000000; i++) {
                final String correlationId = UUID.randomUUID().toString();
                if (i % 1000 == 0) {
                    // Sample each thousandth correlation ID
                    synchronized (lock) {
                        sampledCorrelationIds.add(correlationId);
                    }
                }

                workQueue.execute(() -> {
                    MuProcess process = null;
                    try {
                        process = mngr.newProcess(correlationId);

                        MuActivityParameters parameters = new MuActivityParameters();
                        parameters.put("arg1", "param1");
                        process.execute(
                                (p, r) -> !(Math.random() < /* forward failure probability */ 0.01),
                                parameters
                        );

                        parameters.put("arg2", 42);
                        process.execute(
                                (p, r) -> r.add(10 * (int) p.get("arg2")),
                                new SecondActivityCompensation(),
                                parameters
                        );

                        parameters.put("arg3", true);
                        process.execute(new ThirdActivity(), parameters);

                        parameters.put("arg4", 22 / 7.0);
                        process.execute(new FourthActivity(), parameters);

                        MuProcessResult result = process.getResult();
                        result.add("Adding to the process result");

                        process.finished();

                    } catch (MuProcessBackwardBehaviourException mpbae) {
                        // Forward activity failed and so did some compensation activities
                        String info = "Process and compensation failure: " + mpbae.getMessage();
                        log.trace(info);

                    } catch (MuProcessForwardBehaviourException mpfae) {
                        // Forward activity failed, but compensations were successful
                        String info = "No success, but managed to compensate: " + mpfae.getMessage();
                        log.trace(info);

                    } catch (Throwable t) {
                        // Other reasons for failure not necessarily related to the activity.
                        //
                        // One nice, but not at all representative, example occurred during a prolonged debugging
                        // session (I went for a walk :) where the process management background activity
                        // correctly recovered the assumed stuck process underneath our feet:
                        //
                        //    org.gautelis.muprocessmanager.MuProcessException: Failed to persist process step:
                        //        DerbySQLIntegrityConstraintViolationException [
                        //          INSERT on table 'MU_PROCESS_STEP' caused a violation of foreign key constraint 'MU_P_S_PROCESS_EX' for key (4975097).
                        //          The statement has been rolled back.
                        //        ], SQLstate(23503), Vendor code(20000)
                        //
                        if (null != process) {
                            process.failed();
                        }

                        String info = "Process failure: " + t.getMessage();
                        log.warn(info, t);
                    }
                });
            }

            do {
                System.out.println("\nProcess result samples: ");
                try {
                    synchronized (lock) {
                        // Iterate since we will modify collection
                        Iterator<String> sit = sampledCorrelationIds.iterator();
                        while (sit.hasNext()) {
                            String correlationId  = sit.next();

                            final StringBuffer info = new StringBuffer("correlationId=\"").append(correlationId).append("\"");

                            Optional<MuProcessStatus> _status = mngr.getProcessStatus(correlationId);
                            if (_status.isPresent()) {
                                MuProcessStatus status = _status.get();
                                info.append(" status=").append(status);

                                switch (status) {
                                    case SUCCESSFUL:
                                        sit.remove();

                                        // Retrieve process result
                                        Optional<MuProcessResult> _result = mngr.getProcessResult(correlationId);
                                        _result.ifPresent(objects -> objects.forEach((v) -> info.append(" {").append(v).append("}")));
                                        break;

                                    case NEW:
                                    case PROGRESSING:
                                        // Check later
                                        break;

                                    case COMPENSATED:
                                    case COMPENSATION_FAILED:
                                    default:
                                        sit.remove();

                                        // We will try to reset the process here -- faking a retry
                                        Optional<Boolean> isReset = mngr.resetProcess(correlationId);
                                        isReset.ifPresent(aBoolean -> info.append(" (was ").append(aBoolean ? "" : "NOT ").append("reset)"));
                                        break;
                                }
                                System.out.println(info);
                            }
                        }
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
            System.out.println(info);
            log.warn(info);
        }
        catch (Throwable t) {
            String info = "Failure: ";
            info += t;
            System.out.println(info);
            log.warn(info, t);
        }
    }
}
