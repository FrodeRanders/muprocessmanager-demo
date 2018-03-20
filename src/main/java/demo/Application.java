package demo;

import org.gautelis.muprocessmanager.*;
import org.gautelis.muprocessmanager.payload.MuNativeActivityParameters;
import org.gautelis.muprocessmanager.payload.MuNativeProcessResult;
import org.gautelis.vopn.db.Database;
import org.gautelis.vopn.db.utils.Derby;
import org.gautelis.vopn.db.utils.PostgreSQL;
import org.gautelis.vopn.queue.WorkQueue;
import org.gautelis.vopn.queue.WorkerQueueFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.io.IOException;
import java.io.InputStream;
import java.util.*;

/**
 */
public class Application
{
    private static final Logger log = LoggerFactory.getLogger(Application.class);

    private static final Object lock = new Object();

    private enum DatabaseBackend {
        internal,
        derby,
        db2,
        postgresql
    };

    private static DatabaseBackend backend = DatabaseBackend.internal;

    public static void main( String... args )
    {
        System.out.println("Backing database: " + backend.name());
        try {
            MuProcessManager mngr;

            try {
                final DataSource dataSource;
                switch (backend) {
                    case postgresql:
                        try (InputStream is = Application.class.getResourceAsStream(backend.name() + "-configuration.xml")) {
                            Properties properties = new Properties();
                            properties.loadFromXML(is);

                            dataSource = PostgreSQL.getDataSource("demo", Database.getConfiguration(properties));
                            mngr = MuProcessManager.getManager(dataSource);
                        }
                        break;

                    case derby:
                    case db2:
                        try (InputStream is = Application.class.getResourceAsStream(backend.name() + "-configuration.xml")) {
                            Properties properties = new Properties();
                            properties.loadFromXML(is);

                            dataSource = Derby.getDataSource("demo", Database.getConfiguration(properties));
                            mngr = MuProcessManager.getManager(dataSource);
                        }
                        break;

                    case internal:
                    default:
                        mngr = MuProcessManager.getManager();
                        break;
                }

                mngr.start();

            } catch (IOException ioe) {
                String info = "No configuration for database: " + backend.name() + ": ";
                info += ioe.getMessage();
                throw new MuProcessException(info, ioe);
            }


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

                        MuNativeActivityParameters parameters = new MuNativeActivityParameters();
                        parameters.put("arg1", "param1");
                        process.execute(
                                c -> !(Math.random() < /* forward failure probability */ 0.01),
                                parameters
                        );

                        parameters.put("arg2", 42);
                        process.execute(
                                c -> {
                                    if (c.usesNativeDataFlow()) {
                                        MuNativeActivityParameters np = (MuNativeActivityParameters)c.getActivityParameters();
                                        MuNativeProcessResult nr = (MuNativeProcessResult)c.getResult();
                                        nr.add(10 * (int) np.get("arg2"));
                                    }
                                    return true;
                                },
                                new SecondActivityCompensation(),
                                parameters
                        );

                        parameters.put("arg3", true);
                        process.execute(new ThirdActivity(), parameters);

                        parameters.put("arg4", 22 / 7.0);
                        process.execute(new FourthActivity(), parameters);

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
                System.out.println("\nProcess result samples: " + sampledCorrelationIds.size());
                try {
                    synchronized (lock) {
                        // Iterate since we will modify collection
                        Iterator<String> sit = sampledCorrelationIds.iterator();
                        while (sit.hasNext()) {
                            String correlationId  = sit.next();

                            final StringBuffer info = new StringBuffer("correlationId=\"").append(correlationId).append("\"");

                            Optional<MuProcessState> _state = mngr.getProcessState(correlationId);
                            if (_state.isPresent()) {
                                MuProcessState state = _state.get();
                                info.append(" state=").append(state);

                                switch (state) {
                                    case SUCCESSFUL:
                                        sit.remove();

                                        // Retrieve process result
                                        Optional<MuProcessResult> _result = mngr.getProcessResult(correlationId);
                                        _result.ifPresent(objects -> {
                                            if (objects.isNative()) {
                                                ((MuNativeProcessResult)objects).forEach(v -> info.append(" {").append(v).append("}"));
                                            }
                                        });
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
            t.printStackTrace(System.err);
            log.warn(info, t);
        }
    }
}
