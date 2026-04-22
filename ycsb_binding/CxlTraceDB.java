package com.cxl.consensus;

import site.ycsb.ByteIterator;
import site.ycsb.DB;
import site.ycsb.DBException;
import site.ycsb.Status;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.Vector;

/**
 * YCSB binding that records operations into per-node dataset files for the CXL consensus replay harness.
 *
 * <p>Supported workload operations: inserts -> PUT (P), updates -> UPDATE (U), reads -> GET (G), deletes -> DELETE (D).</p>
 * <p>Scans are not supported and will return NOT_IMPLEMENTED.</p>
 *
 * Configuration (pass with -p):
 * cxl.leaderfile      : path to leader dataset file (default leader_dataset.txt)
 * cxl.follower1file   : follower1 dataset path (default follower1_dataset.txt)
 * cxl.follower2file   : follower2 dataset path (default follower2_dataset.txt)
 * cxl.follower3file   : follower3 dataset path (default follower3_dataset.txt)
 * cxl.followerprefix  : prefix for auto-generated follower files (default follower)
 * cxl.nodes           : number of nodes (default 3, supports 1-4)
 * cxl.readtoleader    : 1 to round-robin reads across all nodes, 0 to send reads to followers only (default 1)
 * cxl.multinode_write : 1 to distribute writes across all nodes, 0 to send writes to leader only (default 1)
 * cxl.preloadcount    : force first N ops to be PUT writes (default 0)
 * cxl.qps             : target QPS used to generate Poisson timestamps (default 1000)
 * cxl.seed            : RNG seed for Poisson timestamp generation (default 1)
 * cxl.keybytes        : fixed key length in bytes/chars (default 64)
 * cxl.valuebytes      : fixed value length in bytes/chars for PUT/UPDATE (default 256)
 * cxl.truncate        : true to truncate dataset files on first init (default true)
 * cxl.maxpayload      : maximum payload bytes including "<op> key:" prefix (default 63)
 */
public class CxlTraceDB extends DB {
    private static final int DEFAULT_MAX_PAYLOAD = 10*1024;
     private static final int DEFAULT_KEY_BYTES = 64;
     private static final int DEFAULT_VALUE_BYTES = 256;

    private static final Object INIT_LOCK = new Object();
    private static final AtomicBoolean INITIALIZED = new AtomicBoolean(false);
    private static final AtomicInteger INSTANCE_COUNT = new AtomicInteger(0);
    private static final AtomicLong READ_COUNT = new AtomicLong(0);
    private static final AtomicLong WRITE_COUNT = new AtomicLong(0);
    private static final AtomicLong OP_COUNT = new AtomicLong(0);

    private static final Object TS_LOCK = new Object();
    private static double timestampAccUs = 0.0;
    private static long lastEmittedTimestampUs = 0;
     private static double qps = 1000.0;
     private static long preloadCount = 0;
     private static boolean multinodeWrite = true;
     private static int keyBytes = DEFAULT_KEY_BYTES;
     private static int valueBytes = DEFAULT_VALUE_BYTES;
     private static java.util.Random rng = new java.util.Random(1);

    private static BufferedWriter[] writers;
    private static Object[] writerLocks;
    private static String[] fileNames;
    private static boolean readToLeader = true;
    private static int nodes = 3;
    private static int maxPayload = DEFAULT_MAX_PAYLOAD;

     private static String fixedValue = null;

    @Override
    public void init() throws DBException {
        synchronized (INIT_LOCK) {
            if (!INITIALIZED.get()) {
                Properties props = getProperties();
                nodes = Integer.parseInt(props.getProperty("cxl.nodes", "3"));
                if (nodes < 1 || nodes > 4) {
                    throw new DBException("cxl.nodes must be between 1 and 4 (got " + nodes + ")");
                }

                readToLeader = !"0".equals(props.getProperty("cxl.readtoleader", "1"));
                multinodeWrite = !"0".equals(props.getProperty("cxl.multinode_write", "1"));

                preloadCount = Long.parseLong(props.getProperty("cxl.preloadcount", "0"));
                qps = Double.parseDouble(props.getProperty("cxl.qps", "1000"));
                long seed = Long.parseLong(props.getProperty("cxl.seed", "1"));
                rng = new java.util.Random(seed);

                // Reset timestamp generator for a fresh dataset.
                synchronized (TS_LOCK) {
                    timestampAccUs = 0.0;
                    lastEmittedTimestampUs = 0;
                }

                keyBytes = Integer.parseInt(props.getProperty("cxl.keybytes", String.valueOf(DEFAULT_KEY_BYTES)));
                valueBytes = Integer.parseInt(props.getProperty("cxl.valuebytes", String.valueOf(DEFAULT_VALUE_BYTES)));
                if (keyBytes < 1) {
                    keyBytes = 1;
                }
                if (valueBytes < 0) {
                    valueBytes = 0;
                }
                fixedValue = buildFixedValue(valueBytes);

                maxPayload = Integer.parseInt(props.getProperty("cxl.maxpayload", String.valueOf(DEFAULT_MAX_PAYLOAD)));

                String leaderFile = props.getProperty("cxl.leaderfile", "leader_dataset.txt");
                String follower1 = props.getProperty("cxl.follower1file", "follower1_dataset.txt");
                String follower2 = props.getProperty("cxl.follower2file", "follower2_dataset.txt");
                String follower3 = props.getProperty("cxl.follower3file", "follower3_dataset.txt");
                String followerPrefix = props.getProperty("cxl.followerprefix", "follower");

                List<String> outputs = new ArrayList<>();
                outputs.add(leaderFile);
                if (nodes > 1) {
                    outputs.add(follower1);
                }
                if (nodes > 2) {
                    outputs.add(follower2);
                }
                if (nodes > 3) {
                    outputs.add(follower3.isEmpty() ? followerPrefix + "3_dataset.txt" : follower3);
                }

                writers = new BufferedWriter[nodes];
                writerLocks = new Object[nodes];
                fileNames = new String[nodes];

                boolean truncate = Boolean.parseBoolean(props.getProperty("cxl.truncate", "true"));
                for (int i = 0; i < nodes; ++i) {
                    String path = outputs.get(i);
                    File f = new File(path);
                    File parent = f.getParentFile();
                    if (parent != null && !parent.exists()) {
                        //noinspection ResultOfMethodCallIgnored
                        parent.mkdirs();
                    }
                    if (truncate && f.exists()) {
                        if (!f.delete()) {
                            throw new DBException("Failed to truncate dataset file: " + path);
                        }
                    }
                    try {
                        writers[i] = new BufferedWriter(new FileWriter(f, true));
                    } catch (IOException e) {
                        throw new DBException("Failed to open dataset file: " + path, e);
                    }
                    writerLocks[i] = new Object();
                    fileNames[i] = path;
                }

                INITIALIZED.set(true);
            }
            INSTANCE_COUNT.incrementAndGet();
        }
    }

    @Override
    public void cleanup() throws DBException {
        synchronized (INIT_LOCK) {
            if (INSTANCE_COUNT.decrementAndGet() == 0) {
                if (writers != null) {
                    for (BufferedWriter writer : writers) {
                        if (writer != null) {
                            try {
                                writer.flush();
                                writer.close();
                            } catch (IOException e) {
                                throw new DBException("Failed to close dataset writer", e);
                            }
                        }
                    }
                }
                System.err.println("[CXL] Dataset generation complete: ops=" + OP_COUNT.get()
                        + " writes=" + WRITE_COUNT.get() + " reads=" + READ_COUNT.get());
                for (int i = 0; i < fileNames.length; ++i) {
                    System.err.println("[CXL] Node " + (i + 1) + " dataset -> " + fileNames[i]);
                }
                INITIALIZED.set(false);
            }
        }
    }

    @Override
    public Status insert(String table, String key, Map<String, ByteIterator> values) {
        return writeOp("P", key); // PUT -> 'P'
    }

    @Override
    public Status read(String table, String key, Set<String> fields, Map<String, ByteIterator> result) {
        long opIndex = OP_COUNT.getAndIncrement();
        String normKey = normalizeKey(key);

        try {
            if (preloadCount > 0 && opIndex < preloadCount) {
                // Force initial ops to be PUT so replay preloading always finds enough writes.
                WRITE_COUNT.incrementAndGet();
                int targetNode = chooseWriteNode();
                appendLine(targetNode, buildPayload("P", normKey, fixedValue));
            } else {
                READ_COUNT.incrementAndGet();
                int targetNode = chooseReadNode();
                appendLine(targetNode, "G " + normKey); // GET -> 'G'
            }
            return Status.OK;
        } catch (DBException e) {
            e.printStackTrace();
            return Status.ERROR;
        }
    }

    @Override
    public Status update(String table, String key, Map<String, ByteIterator> values) {
        return writeOp("P", key); // UPDATE -> 'P' (Forced PUT)
    }

    @Override
    public Status delete(String table, String key) {
        OP_COUNT.getAndIncrement();
        WRITE_COUNT.incrementAndGet();
        try {
            int targetNode = chooseWriteNode();
            appendLine(targetNode, "D " + normalizeKey(key)); // DELETE -> 'D'
            return Status.OK;
        } catch (DBException e) {
            e.printStackTrace();
            return Status.ERROR;
        }
    }

    @Override
    public Status scan(String table, String startkey, int recordcount, Set<String> fields,
                       Vector<HashMap<String, ByteIterator>> result) {
        return Status.NOT_IMPLEMENTED;
    }

    private Status writeOp(String verb, String key) {
        OP_COUNT.getAndIncrement();
        WRITE_COUNT.incrementAndGet();
        String normKey = normalizeKey(key);
        String payload = buildPayload(verb, normKey, fixedValue);
        try {
            int targetNode = chooseWriteNode();
            appendLine(targetNode, payload);
            return Status.OK;
        } catch (DBException e) {
            e.printStackTrace();
            return Status.ERROR;
        }
    }

    private static String buildFixedValue(int bytes) {
        if (bytes <= 0) {
            return "";
        }
        char[] buf = new char[bytes];
        for (int i = 0; i < bytes; ++i) {
            buf[i] = 'v';
        }
        return new String(buf);
    }

    private static String normalizeKey(String key) {
        if (key == null) {
            key = "";
        }
        if (key.length() == keyBytes) {
            return key;
        }
        if (key.length() > keyBytes) {
            return key.substring(0, keyBytes);
        }
        StringBuilder sb = new StringBuilder(keyBytes);
        sb.append(key);
        while (sb.length() < keyBytes) {
            sb.append('0');
        }
        return sb.toString();
    }

    private static String buildPayload(String verb, String key, String value) {
        if (value == null) {
            value = "";
        }
        int maxValueLen = maxPayload - (verb.length() + 1 + key.length() + 1);
        if (maxValueLen < 1) {
            maxValueLen = 1;
        }
        if (value.length() > maxValueLen) {
            value = value.substring(0, maxValueLen);
        }
        return verb + " " + key + ":" + value;
    }

    private static int chooseReadNode() {
        if (nodes <= 1) {
            return 0;
        }
        long counter = READ_COUNT.get();
        if (readToLeader) {
            return (int) (counter % nodes);
        }
        return 1 + (int) (counter % (nodes - 1));
    }

    private static int chooseWriteNode() {
        if (nodes <= 1) {
            return 0;
        }
        if (!multinodeWrite) {
            return 0;
        }
        long counter = WRITE_COUNT.get();
        return (int) (counter % nodes);
    }

    private static long nextTimestampUs() {
        synchronized (TS_LOCK) {
            if (qps <= 0.0) {
                return lastEmittedTimestampUs;
            }
            // Exponential inter-arrival time for Poisson process.
            double u = rng.nextDouble();
            if (u <= 0.0) {
                u = 1e-12;
            }
            double deltaUs = (-Math.log(1.0 - u) / qps) * 1_000_000.0;
            if (deltaUs < 0.0) {
                deltaUs = 0.0;
            }

            // Accumulate in double to avoid systematic rounding bias when mean inter-arrival < 1us.
            timestampAccUs += deltaUs;

            long ts = (long) Math.floor(timestampAccUs);
            if (ts < lastEmittedTimestampUs) {
                ts = lastEmittedTimestampUs;
            } else {
                lastEmittedTimestampUs = ts;
            }
            return ts;
        }
    }

    private static void appendLine(int nodeIndex, String line) throws DBException {
        if (nodeIndex < 0 || nodeIndex >= writers.length) {
            throw new DBException("Invalid node index " + nodeIndex);
        }
        BufferedWriter writer = writers[nodeIndex];
        Object lock = writerLocks[nodeIndex];
        try {
            synchronized (lock) {
                long ts = nextTimestampUs();
                writer.write(Long.toString(ts));
                writer.write(',');
                writer.write(line);
                writer.newLine();
            }
        } catch (IOException e) {
            throw new DBException("Failed to write dataset line", e);
        }
    }
}
