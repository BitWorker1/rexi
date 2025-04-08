package work.bit.rexi;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Base64;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.opencsv.CSVReader;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.resps.ScanResult;
import redis.clients.jedis.resps.Tuple;

/*
 * This class is responsible for importing and exporting keys from Redis to a CSV file.
 * It uses the Jedis Java client library for Redis.
 *
 * Usage:
 * 1. Create a new instance of KeyBatchTool with your Redis host and port.
 * 2. Call the exportKeys() method to export all keys from Redis to a CSV file.
 * 
 * CSV format:
 *  key,type,keyOp,elemOp
 * key - key name
 * type - string, set, zset, hash ,list,zindex
 * keyOp - 
 *  MRG - merge - insert and replace existing keys (DEFAULT)
 *  RPL - replace - must exist
 *  DEL - delete - remove if exists
 *  INS - insert - insert a new element must not exists
 *  CMP - compare - compare keys list t he one that don't exist in the target database
 *  
 * elemOp - determines the operation to be performed on the elements of the set, zset, list, string or hash.
 * MRG - merge - insert and replace existing elements (DEFAULT)
 * RPL - replace - must exist
 * DEL - delete - remove if exists
 * INS - insert - insert a new element must not exists
 * CMP - compare - compare elements of the key list t he one that don't exist in the target key
 * 
 * the data is exported as a base64 encoded string.
 */
public class KeyBatchTool {
    private Jedis jedis;
    private String redisHost;
    private int redisPort;
    private int redisDatabase;
    private String redisAuth;
    private String csvFilePath;
    private String datFilePath;
    private String fieldSeperator = ",=";

    public KeyBatchTool(String host, int port, int database) {
        this(host, port, database, null);
    }

    public KeyBatchTool(String host, int port, int database, String auth) {
        this.redisHost = host;
        this.redisPort = port;
        this.redisDatabase = database;
        this.redisAuth = auth;
        connectToRedis();
    }

    public void setCsvFilePath(String path) {
        this.csvFilePath = path;
    }

    public void setDatFilePath(String path) {
        this.datFilePath = path;
    }

    private void connectToRedis() {
        jedis = new Jedis(redisHost, redisPort);
        if (redisAuth != null && !redisAuth.isEmpty()) {
            jedis.auth(redisAuth);
        }
        jedis.select(redisDatabase);
    }

    public void switchDatabase(int newDatabase) {
        this.redisDatabase = newDatabase;
        jedis.select(redisDatabase);
    }

    private static Logger logger = LoggerFactory.getLogger(KeyBatchTool.class);

    private void clearFile(String fileName) throws Exception {
        new FileOutputStream(fileName).close();
    }

    public void exportKey(String keyName, String type, String keyOp, String elemOp, String keyPrefix, String keySuffix)
            throws Exception {
        System.out.println(type + "::" + keyName);
        String data = "";
        String dataCMD = keyName + "," + type + "," + keyOp + "," + elemOp + ",";

        switch (type) {
            case "zzindex":
                ScanResult<Tuple> zziscanZResults = jedis.zscan(keyName, "0");
                StringBuilder zzisb = new StringBuilder();
                for (Tuple i : zziscanZResults.getResult()) {
                    zzisb.append(i.getElement()).append(":").append(Long.toUnsignedString((long) i.getScore()))
                            .append(fieldSeperator);
                    exportKey(keyPrefix + i.getElement() + keySuffix, "zindex", keyOp, elemOp, "", "");
                }
                data = zzisb.toString();
                break;
            case "zindex":
                ScanResult<Tuple> ziscanZResults = jedis.zscan(keyName, "0");
                StringBuilder zisb = new StringBuilder();
                for (Tuple i : ziscanZResults.getResult()) {
                    zisb.append(i.getElement()).append(":").append(Long.toUnsignedString((long) i.getScore()))
                            .append(fieldSeperator);
                    exportKey(keyPrefix + i.getElement() + keySuffix, "hash", keyOp, elemOp, keyPrefix, keySuffix);
                }
                data = zisb.toString();
                break;
            case "zset":
                ScanResult<Tuple> scanZResults = jedis.zscan(keyName, "0");
                StringBuilder zsb = new StringBuilder();
                for (Tuple i : scanZResults.getResult()) {
                    zsb.append(i.getElement()).append(":").append(Long.toUnsignedString((long) i.getScore()))
                            .append(fieldSeperator);
                    // exportKey(i.getElement(), "hash", keyOp, elemOp);
                }
                data = zsb.toString();
                break;
            case "set":
                Set<String> smembers = new HashSet<>();
                ScanResult<String> scanResults = jedis.sscan(keyName, "0");
                Iterator<String> smemberIterator = scanResults.getResult().iterator();
                while (smemberIterator.hasNext()) {
                    smembers.add(smemberIterator.next());
                }
                StringBuilder ssb = new StringBuilder();
                for (String member : smembers) {
                    if (ssb.length() > 0)
                        ssb.append(fieldSeperator);
                    ssb.append("'").append(member).append("'");
                }
                data = ssb.toString();
                break;

            case "string":
                String strValue = jedis.get(keyName);
                data = strValue != null ? strValue : "";
                break;
            case "hash":
                Map<String, String> fields = new HashMap<>();
                ScanResult<Map.Entry<String, String>> scanHResults = jedis.hscan(keyName, "0");
                Iterator<Map.Entry<String, String>> fieldIterator = scanHResults.getResult().iterator();
                while (fieldIterator.hasNext()) {
                    Map.Entry<String, String> entry = fieldIterator.next();
                    fields.put(entry.getKey(), entry.getValue());
                }
                StringBuilder sb = new StringBuilder();
                for (Map.Entry<String, String> entry : fields.entrySet()) {
                    if (sb.length() > 0)
                        sb.append(fieldSeperator);
                    String sEntry = entry.getValue();
                    sb.append("'").append(entry.getKey()).append(":").append(sEntry).append("'");
                }
                data = sb.toString();
                break;
            case "list":
                List<String> listMembers = new ArrayList<>();
                long listSize = jedis.llen(keyName);
                int batchSize = 100;
                for (long start = 0; start < listSize; start += batchSize) {
                    long end = Math.min(start + batchSize - 1, listSize - 1);
                    List<String> batch = jedis.lrange(keyName, start, end);
                    listMembers.addAll(batch);
                }
                data = String.join(fieldSeperator, listMembers);
                break;
            default:
                logger.warn("Unknown key type: {}", type);
                return;
        }
        String encodedData = Base64.getEncoder().encodeToString(data.getBytes("UTF-8"));
        String datLine = dataCMD + encodedData + "\n";
        try (FileOutputStream fos = new FileOutputStream(datFilePath, true)) {
            fos.write(datLine.getBytes());
        }
    }

    public void exportKeys() throws Exception {
        clearFile(datFilePath);
        CSVReader csvReader = new CSVReader(new InputStreamReader(
                new FileInputStream(csvFilePath), "UTF-8"));
        // csvReader.setStrict(false);
        System.out.println("Reading keys from CSV...");
        List<String[]> records = csvReader.readAll();
        csvReader.close();
        for (String[] record : records) {
            String keyName = record[0];
            if (keyName.startsWith("#"))
                continue;
            if (record.length < 3)
                continue;

            String type = record[1];// jedis.type(keyName);
            String keyOp = record[2];
            String elemOp = record[3] != "" ? record[3] : "MRG";
            String keyPrefix = "";
            String keySuffix = "";
            if (record.length > 4)
                keyPrefix = record[4];
            if (record.length > 5)
                keySuffix = record[5];
            String dataCMD = keyName + "," + type + "," + keyOp + "," + elemOp + ",";
            System.out.println(dataCMD);
            exportKey(keyName, type, keyOp, elemOp, keyPrefix, keySuffix);

        }
    }

    public void importKeys() throws Exception {
        BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(datFilePath), "UTF-8"));
        String line;
        while ((line = reader.readLine()) != null) {
            String[] parts = line.split(",", 5);
            if (parts.length < 5) {
                logger.warn("Invalid line format: {}", line);
                continue;
            }

            String keyName = parts[0];
            String type = parts[1];
            String keyOp = parts[2];
            String elemOp = parts[3];
            String encodedData = parts[4];
            String data = new String(Base64.getDecoder().decode(encodedData), "UTF-8");
            String dataCMD = keyName + "," + type + "," + keyOp + "," + elemOp + ",";
            System.out.println("i>" + dataCMD);
            switch (keyOp) {
                case "MRG":
                    mergeKey(keyName, type, elemOp, data);
                    break;
                case "RPL":
                    replaceKey(keyName, type, elemOp, data);
                    break;
                case "DEL":
                    deleteKey(keyName);
                    break;
                case "INS":
                    insertKey(keyName, type, elemOp, data);
                    break;
                default:
                    logger.warn("Unknown key operation: {}", keyOp);
            }
        }
        reader.close();
    }

    private void mergeKey(String keyName, String type, String elemOp, String data) {
        switch (type) {
            case "zindex":
                mergeZIndex(keyName, data);
                break;
            case "set":
                mergeSet(keyName, data, elemOp);
                break;
            case "zset":
                mergeZSet(keyName, data, elemOp);
                break;
            case "string":
                mergeString(keyName, data);
                break;
            case "hash":
                mergeHash(keyName, data, elemOp);
                break;
            case "list":
                mergeList(keyName, data, elemOp);
                break;
            default:
                logger.warn("Unknown key type for merge: {}", type);
        }
    }

    private void replaceKey(String keyName, String type, String elemOp, String data) {
        jedis.del(keyName);
        mergeKey(keyName, type, elemOp, data);
    }

    private void deleteKey(String keyName) {
        jedis.del(keyName);
    }

    private void insertKey(String keyName, String type, String elemOp, String data) {
        if (!jedis.exists(keyName)) {
            mergeKey(keyName, type, elemOp, data);
        } else {
            logger.warn("Key already exists, cannot insert: {}", keyName);
        }
    }

    private void mergeZIndex(String keyName, String data) {
        String[] pairs = data.split(fieldSeperator);
        for (String pair : pairs) {
            String[] parts = pair.split(":");
            if (parts.length == 2) {
                jedis.zadd(keyName, Long.parseUnsignedLong(parts[1]), parts[0]);
            }
        }
    }

    private void mergeZSet(String keyName, String data, String elemOp) {
        String[] members = data.split(fieldSeperator);
        for (String member : members) {
            String[] parts = member.split(":");

            switch (elemOp) {
                case "MRG":
                case "INS":
                    if (parts.length == 2) {
                        jedis.zadd(keyName, Long.parseUnsignedLong(parts[1]), parts[0]);
                    } else {
                        jedis.zadd(keyName, 1, parts[0]);
                    }
                    break;
                case "DEL":
                    jedis.zrem(keyName, parts[0]);
                    break;
                default:
                    logger.warn("Unknown element operation for zset: {}", elemOp);
            }
        }
    }

    private void mergeSet(String keyName, String data, String elemOp) {
        String[] members = data.replaceAll("'", "").split(fieldSeperator);
        for (String member : members) {
            switch (elemOp) {
                case "MRG":
                case "INS":
                    jedis.sadd(keyName, member);
                    break;
                case "DEL":
                    jedis.srem(keyName, member);
                    break;
                default:
                    logger.warn("Unknown element operation for set: {}", elemOp);
            }
        }
    }

    private void mergeString(String keyName, String data) {
        jedis.set(keyName, data);
    }

    private void mergeHash(String keyName, String data, String elemOp) {
        String[] pairs = data.replaceAll("'", "").split(fieldSeperator);
        for (String pair : pairs) {
            String[] parts = pair.split(":", 2);
            if (parts.length == 2) {
                switch (elemOp) {
                    case "MRG":
                    case "INS":
                        jedis.hset(keyName, parts[0], parts[1]);
                        break;
                    case "DEL":
                        jedis.hdel(keyName, parts[0]);
                        break;
                    default:
                        logger.warn("Unknown element operation for hash: {}", elemOp);
                }
            }
        }
    }

    private void mergeList(String keyName, String data, String elemOp) {
        String[] elements = data.split(fieldSeperator);
        switch (elemOp) {
            case "MRG":
            case "INS":
                for (String element : elements) {
                    jedis.rpush(keyName, element);
                }
                break;
            case "DEL":
                for (String element : elements) {
                    jedis.lrem(keyName, 0, element);
                }
                break;
            default:
                logger.warn("Unknown element operation for list: {}", elemOp);
        }
    }

}
