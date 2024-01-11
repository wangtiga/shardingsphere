package com.csplog;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;

public class CspLogCenter {
    // key: trace_id value: 按照时间顺序和trace_id相关的日志
    private static Map<String, LinkedBlockingQueue<String>> logs = new HashMap<String, LinkedBlockingQueue<String>>();


    public synchronized static void pushLog(String trace_id, String log) {
        if (logs.get(trace_id) == null) {
            logs.put(trace_id, new LinkedBlockingQueue<String>(100));
        }
        logs.get(trace_id).add(log);
    }

    public synchronized static String popLog(String trace_id) {
        while (logs.get(trace_id) == null) {
            try {
                Thread.sleep(1);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        String data = logs.get(trace_id).poll();
        if (logs.get(trace_id) != null && logs.get(trace_id).isEmpty()) {
            logs.remove(trace_id);
        }
        return data;
    }

    public synchronized static boolean isEmpty(String trace_id) {
        if (logs.get(trace_id) == null) {
            return true;
        }
        return logs.get(trace_id).isEmpty();
    }
}
