package com.aznauryan;

import com.aznauryan.exception.IllegalStatusException;
import com.aznauryan.streakstats.proto.VeryImportantMeasurementOuterClass;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by Suren on 12/5/2016.
 */
public class StreakStats {

    public static void main(String[] args) throws IOException {
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        VeryImportantMeasurementOuterClass.VeryImportantMeasurement veryImportantMeasurement =
                VeryImportantMeasurementOuterClass.VeryImportantMeasurement.newBuilder().setSessionId("44").setStatus(1).build();

        VeryImportantMeasurementOuterClass.VeryImportantMeasurement veryImportantMeasurement1 =
                VeryImportantMeasurementOuterClass.VeryImportantMeasurement.newBuilder().setSessionId("44").setStatus(1).build();

        VeryImportantMeasurementOuterClass.VeryImportantMeasurement veryImportantMeasurement2 =
                VeryImportantMeasurementOuterClass.VeryImportantMeasurement.newBuilder().setSessionId("44").setStatus(2).build();

        VeryImportantMeasurementOuterClass.VeryImportantMeasurement veryImportantMeasurement3 =
                VeryImportantMeasurementOuterClass.VeryImportantMeasurement.newBuilder().setSessionId("44").setStatus(1).build();

        VeryImportantMeasurementOuterClass.VeryImportantMeasurement veryImportantMeasurement4 =
                VeryImportantMeasurementOuterClass.VeryImportantMeasurement.newBuilder().setSessionId("44").setStatus(2).build();

        VeryImportantMeasurementOuterClass.VeryImportantMeasurement veryImportantMeasurement5 =
                VeryImportantMeasurementOuterClass.VeryImportantMeasurement.newBuilder().setSessionId("4466").setStatus(1).build();

        VeryImportantMeasurementOuterClass.VeryImportantMeasurement veryImportantMeasurement6 =
                VeryImportantMeasurementOuterClass.VeryImportantMeasurement.newBuilder().setSessionId("4466").setStatus(2).build();


        veryImportantMeasurement.writeDelimitedTo(byteArrayOutputStream);
        veryImportantMeasurement1.writeDelimitedTo(byteArrayOutputStream);
        veryImportantMeasurement2.writeDelimitedTo(byteArrayOutputStream);
        veryImportantMeasurement3.writeDelimitedTo(byteArrayOutputStream);
        veryImportantMeasurement4.writeDelimitedTo(byteArrayOutputStream);
        veryImportantMeasurement5.writeDelimitedTo(byteArrayOutputStream);
        veryImportantMeasurement6.writeDelimitedTo(byteArrayOutputStream);

        StreakStats streakStats = StreakStats.getInstance();
        streakStats.processStream(new ByteArrayInputStream(byteArrayOutputStream.toByteArray()));

        Map<String, Map<Long, Long>> map = streakStats.getStatistics();
        map = map;
    }

    /**
     *  initializing eagerly as the current class instance creation is not a heavy process.
     */
    private final static StreakStats instance = new StreakStats();

    private final Map<String, Map<Long, Long>> streakStatsMap = new ConcurrentHashMap<String, Map<Long, Long>>();
    private final Map<String, Long> sessionIdToCurrentStreakStartTimeMap = new ConcurrentHashMap<String, Long>();

    private final Object lock = new Object();

    private StreakStats() {
    }

    public void processStream(InputStream someDataStream) throws IOException {
        VeryImportantMeasurementOuterClass.VeryImportantMeasurement veryImportantMeasurement;
        while ((veryImportantMeasurement =
                VeryImportantMeasurementOuterClass.VeryImportantMeasurement.parseDelimitedFrom(someDataStream)) != null) {

            final String sessionId = veryImportantMeasurement.getSessionId();
            final int status = veryImportantMeasurement.getStatus();

            if (status == StreakStatus.START.getStatus()) {
                if (!sessionIdToCurrentStreakStartTimeMap.containsKey(sessionId)) {
                    synchronized (lock) {
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
                            synchronized (lock) {
                                long currentMessagesCount = streakStatsMap.get(sessionId).get(sessionIdToCurrentStreakStartTimeMap.get(sessionId));
                                streakStatsMap.get(sessionId).put(sessionIdToCurrentStreakStartTimeMap.get(sessionId), ++currentMessagesCount);
                            }
                        }
                    }
                } else {
                    synchronized (lock) {
                        long currentMessagesCount = streakStatsMap.get(sessionId).get(sessionIdToCurrentStreakStartTimeMap.get(sessionId));
                        streakStatsMap.get(sessionId).put(sessionIdToCurrentStreakStartTimeMap.get(sessionId), ++currentMessagesCount);
                    }
                }
            } else if (status == StreakStatus.FINISH.getStatus()) {
                synchronized (lock) {
                    sessionIdToCurrentStreakStartTimeMap.remove(sessionId);
                }
            } else {
                throw new IllegalStatusException(String.format("'VeryImportantMeasurement' message received with invalid status %d", status));
            }
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
