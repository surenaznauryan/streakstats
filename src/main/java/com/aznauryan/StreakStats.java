package com.aznauryan;

import com.aznauryan.exception.IllegalStatusException;
import com.aznauryan.streakstats.proto.VeryImportantMeasurementOuterClass;

import java.io.IOException;
import java.io.InputStream;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Semaphore;

/**
 * Created by Suren on 12/5/2016.
 */
public class StreakStats {

    /**
     * initializing eagerly as the current class instance creation is not a heavy process.
     */
    private final static StreakStats instance = new StreakStats();

    /**
     * Map containing streak statistics. The key of the map indicates message session id, and the value is a map
     * containing streak start time as its key and count of messages with status StreakStatus.START as its value.
     * The messages with status StreakStatus.FINISH are not reflected in message count.
     */
    private final Map<String, Map<Long, Long>> streakStatsMap = new ConcurrentHashMap<String, Map<Long, Long>>();

    /**
     * Map containing session id of the message as its key and that session id's current streak start time as it's value.
     * The entry corresponding to specific session id is being put into the map when the streak for that session id is
     * started. When the current streak is finished the entry is removed.
     */
    private final Map<String, Long> sessionIdToCurrentStreakStartTimeMap = new ConcurrentHashMap<String, Long>();

    /**
     * Map containing session id of the message as its key and a semaphore object for that session id as its value.
     */
    private final Map<String, Semaphore> sessionIdToSemaphoreMap = new ConcurrentHashMap<String, Semaphore>();

    /**
     * Semaphore used for mutual exclusion upon creation of session id specific semaphores and putting them into
     * sessionIdToSemaphoreMap.
     */
    private final Semaphore semaphore = new Semaphore(1);

    private StreakStats() {
    }

    public void processStream(InputStream someDataStream) throws IOException {
        VeryImportantMeasurementOuterClass.VeryImportantMeasurement veryImportantMeasurement;
        while ((veryImportantMeasurement =
                VeryImportantMeasurementOuterClass.VeryImportantMeasurement.parseDelimitedFrom(someDataStream)) != null) {

            final String sessionId = veryImportantMeasurement.getSessionId();
            final int status = veryImportantMeasurement.getStatus();

            if (!sessionIdToSemaphoreMap.containsKey(sessionId)) {
                semaphore.acquireUninterruptibly();
                if (!sessionIdToSemaphoreMap.containsKey(sessionId)) {
                    sessionIdToSemaphoreMap.put(sessionId, new Semaphore(1));
                }
                semaphore.release();
            }

            sessionIdToSemaphoreMap.get(sessionId).acquireUninterruptibly();
            if (status == StreakStatus.START.getStatus()) {
                if (!sessionIdToCurrentStreakStartTimeMap.containsKey(sessionId)) {
                    if (!streakStatsMap.containsKey(sessionId)) {
                        streakStatsMap.put(sessionId, new HashMap<Long, Long>());
                    }

                    final Long currentTimeMillis = new Date().getTime();

                    /**
                     * The bellow "if" statement guarantees that streak stats will not be overwritten in case when
                     * the streak has started, finished and then again started during the same millisecond. If
                     * such case occurs then any message after the streak has finished is not reflected
                     * in the stats.
                     */
                    if (!streakStatsMap.get(sessionId).containsKey(currentTimeMillis)) {
                        streakStatsMap.get(sessionId).put(currentTimeMillis, 1L);
                        sessionIdToCurrentStreakStartTimeMap.put(sessionId, currentTimeMillis);
                    }
                } else {
                    // increment count in stats.
                    long currentMessagesCount = streakStatsMap.get(sessionId).get(sessionIdToCurrentStreakStartTimeMap.get(sessionId));
                    streakStatsMap.get(sessionId).put(sessionIdToCurrentStreakStartTimeMap.get(sessionId), ++currentMessagesCount);
                }
            } else if (status == StreakStatus.FINISH.getStatus()) {
                // messages with statuses StreakStatus.FINISH do not increment count in stats.
                sessionIdToCurrentStreakStartTimeMap.remove(sessionId);
            } else {
                sessionIdToSemaphoreMap.get(sessionId).release();
                throw new IllegalStatusException(String.format("'VeryImportantMeasurement' message received with invalid status %d", status));
            }
            sessionIdToSemaphoreMap.get(sessionId).release();
        }
    }

    public Map<String, Map<Long, Long>> getStatistics() {
        return Collections.unmodifiableMap(streakStatsMap);
    }

    public static StreakStats getInstance() {
        return instance;
    }

    private enum StreakStatus {
        START(1), FINISH(2);

        private int status;

        StreakStatus(int status) {
            this.status = status;
        }

        public int getStatus() {
            return status;
        }
    }
}
