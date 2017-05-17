package iiis.systems.os.blockdb;

import java.util.HashMap;
import java.util.concurrent.Semaphore;
import java.util.regex.*;
import java.io.*;
import com.google.protobuf.util.JsonFormat;
import org.json.simple.parser.*;
import org.json.simple.*;
import org.json.JSONObject;

public class DatabaseEngine {
    private static DatabaseEngine instance = null;

    public static DatabaseEngine getInstance() {
        return instance;
    }

    public static void setup(String dataDir) {
        instance = new DatabaseEngine(dataDir);
    }

    private HashMap<String, Integer> balances = new HashMap<>();
    private long logLength = 0;
    private String dataDir;

    private long blockId = 1;
    final int N = 3/*50*/;

    final int userIdLength = 8;
    final String template = "[a-z0-9|-]{" + userIdLength + "}";
    Pattern pattern = Pattern.compile(template, Pattern.CASE_INSENSITIVE);

    Block.Builder blockBuilder = Block.newBuilder();
    String serverLogInfoPath;
    org.json.simple.JSONObject serverInfoJson;

    private Semaphore semaphore = new Semaphore(1);

    DatabaseEngine(String dataDir) {
        this.dataDir = dataDir;
        serverLogInfoPath = dataDir + "serverLogInfo.json";

        // create/open a JSON file, stores information including:
        // 1. boolean flag : indicates whether first run
        // 2. int completedBlockNumber : indicates how many blocks we have, updated consistently as id increases
        // 3. int transientLogEntries : indicates how many entries there are in the transient log

        JSONParser parser = new JSONParser();
        File dir = new File(dataDir);
        boolean successful = dir.mkdir();
        if (successful) {
            // creating the directory succeeded
            System.out.println("directory was created successfully");
        }
        else {
            // creating the directory failed
            System.out.println("failed trying to create the directory");
        }

        try {
            Object obj = parser.parse(new FileReader(serverLogInfoPath));

            serverInfoJson = (org.json.simple.JSONObject) obj;
            System.out.println("serverInfoJson");
            System.out.println(serverInfoJson);

            boolean cleanStart = (boolean) serverInfoJson.get("cleanStart");

            if (cleanStart == false) {
                System.out.println("this is not a clean start...");
                System.out.println("trying to restore...!!");
                this.blockId = (long) serverInfoJson.get("completedBlockNumber");
                ++this.blockId;
                this.logLength = (long) serverInfoJson.get("transientLogEntries");

                try {
                    String jsonPath = dataDir + this.blockId + ".json";
                    FileReader fileReader = new FileReader(jsonPath);
                    JsonFormat.parser().merge(fileReader, blockBuilder);
                } catch (FileNotFoundException e) {
                    System.out.println("cannot find transient log, it has not been created yet");
                }

                runRestore();
            }

            /*String name = (String) jsonObject.get("name");
            System.out.println(name);

            long age = (Long) jsonObject.get("age");
            System.out.println(age);*/

            // loop array
            /*JSONArray msg = (JSONArray) jsonObject.get("messages");
            Iterator<String> iterator = msg.iterator();
            while (iterator.hasNext()) {
                System.out.println(iterator.next());
            }*/

        } catch (FileNotFoundException e) {
            //If the file is not found, then it *probably* means this is a cold start
            //e.printStackTrace();
            serverInfoJson = new org.json.simple.JSONObject();
            serverInfoJson.put("cleanStart", false);
            serverInfoJson.put("completedBlockNumber", 0);
            serverInfoJson.put("transientLogEntries", 0);

            try (FileWriter file = new FileWriter(serverLogInfoPath)) {

                file.write(serverInfoJson.toString());
                file.flush();
                System.out.println("created serverLogInfoPath...!!");
                System.out.println(serverInfoJson);

            } catch (IOException exp) {
                exp.printStackTrace();
            }



        } catch (IOException e) {
            e.printStackTrace();
        } catch (ParseException e) {
            e.printStackTrace();
        }

    }

    private int getOrZero(String userId) {
        if (balances.containsKey(userId)) {
            return balances.get(userId);
        } else {
            return 0;
        }
    }

    public int get(String userId) {
        //logLength++;
        try {
            semaphore.acquire();
            int value = getOrZero(userId);
            semaphore.release();
            return value;
        } catch (Exception e) {
            e.printStackTrace();
        }
        return 0;
    }

    public boolean put(String userId, int value) {
        try {
            semaphore.acquire();
            if (pattern.matcher(userId).matches() && value >= 0) {
                writeTransactionLog(Transaction.newBuilder().setType(Transaction.Types.PUT).setUserID(userId).setValue(value).build());
                balances.put(userId, value);
                semaphore.release();
                return true;
            }
            semaphore.release();
            return false;
        } catch (Exception e) {
            e.printStackTrace();
        }
        return false;
    }

    public boolean deposit(String userId, int value) {
        try {
            semaphore.acquire();
            if (pattern.matcher(userId).matches() && value >= 0) {
                writeTransactionLog(Transaction.newBuilder().setType(Transaction.Types.DEPOSIT).setUserID(userId).setValue(value).build());
                int balance = getOrZero(userId);
                balances.put(userId, balance + value);
                semaphore.release();
                return true;
            }
            semaphore.release();
            return false;
        } catch (Exception e) {
            e.printStackTrace();
        }
        return false;
    }

    public boolean withdraw(String userId, int value) {
        try {
            semaphore.acquire();
            if (pattern.matcher(userId).matches() && value >= 0) {
                int balance = getOrZero(userId);
                if (value <= balance) {
                    writeTransactionLog(Transaction.newBuilder().setType(Transaction.Types.WITHDRAW).setUserID(userId).setValue(value).build());
                    balances.put(userId, balance - value);
                    semaphore.release();
                    return true;
                }
            }
            semaphore.release();
            return false;
        } catch (Exception e) {
            e.printStackTrace();
        }
        return false;
    }

    public boolean transfer(String fromId, String toId, int value) {
        try {
            semaphore.acquire();
            if (pattern.matcher(fromId).matches() && pattern.matcher(toId).matches() && !fromId.equals(toId) && value >= 0) {
                int fromBalance = getOrZero(fromId);
                if (value <= fromBalance) {
                    writeTransactionLog(Transaction.newBuilder().setType(Transaction.Types.TRANSFER).setFromID(fromId).setToID(toId).setValue(value).build());
                    int toBalance = getOrZero(toId);
                    balances.put(fromId, fromBalance - value);
                    balances.put(toId, toBalance + value);
                    semaphore.release();
                    return true;
                }
            }
            semaphore.release();
            return false;
        } catch (Exception e) {
            e.printStackTrace();
        }
        return false;
    }

    public int getLogLength() {
        return (int)logLength;
    }

    public void writeTransactionLog(Transaction transaction) {
        if (logLength == 0)
            blockBuilder.setBlockID((int)(blockId)).setPrevHash("00000000").clearTransactions().setNonce("00000000");
        blockBuilder.addTransactions(transaction);
        writeFile(dataDir + blockId + ".json", blockBuilder.build());
        logLength++;
        logLength%=N;

        if (logLength == 0) {
            ++blockId;
        }

        serverInfoJson.put("completedBlockNumber", blockId - 1);
        serverInfoJson.put("transientLogEntries", logLength);
        try (FileWriter file = new FileWriter(serverLogInfoPath))
        {
            file.write(serverInfoJson.toString());
            System.out.println("Successfully updated json object to file...!!");
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    private boolean runRestore() {
        try {
            semaphore.acquire();

            JSONObject serverLogJson = Util.readJsonFile(serverLogInfoPath);
            int completeBlocks = serverLogJson.getInt("completedBlockNumber");
            int transientLogEntries = serverLogJson.getInt("transientLogEntries");

            //restore the completed json blocks
            for (int i = 1; i <= completeBlocks; ++i) {
                String jsonPath = dataDir + i + ".json";
                //JSONObject jsonblock = Util.readJsonFile(jsonPath);
                FileReader fileReader = new FileReader(jsonPath);
                Block.Builder builder = Block.newBuilder();
                JsonFormat.parser().merge(fileReader, builder);

                for (int j = 0 ; j < N; ++j) {
                    System.out.println("restoring block: " + i + " transaction: " + j);
                    Transaction thisTransaction = builder.getTransactions(j);
                    restoreThisTransaction(thisTransaction);
                }
            }

            //restore transient logs
            if (transientLogEntries > 0) {
                int transientId = completeBlocks + 1;
                String jsonPath = dataDir + transientId + ".json";
                FileReader fileReader = new FileReader(jsonPath);
                Block.Builder builder = Block.newBuilder();
                JsonFormat.parser().merge(fileReader, builder);
                for (int i = 0; i < transientLogEntries; ++i) {
                    System.out.println("restoring transient log: " + i);
                    Transaction thisTransaction = builder.getTransactions(i);
                    restoreThisTransaction(thisTransaction);
                }
            }

            semaphore.release();
            return true;
        } catch (Exception e) {
            e.printStackTrace();
        }
        return false;
    }

    private boolean restoreThisTransaction(Transaction thisTransaction) {
        String userId = thisTransaction.getUserID();
        String fromId = thisTransaction.getFromID();
        String toId = thisTransaction.getToID();
        int value = thisTransaction.getValue();
        Transaction.Types type = thisTransaction.getType();
        int balance;

        switch (type) {
            case PUT:
                balances.put(userId, value);
                break;
            case DEPOSIT:
                balance = getOrZero(userId);
                balances.put(userId, balance + value);
                break;
            case WITHDRAW:
                balance = getOrZero(userId);
                if (value <= balance) {
                    balances.put(userId, balance - value);
                }
                break;
            case TRANSFER:
                int fromBalance = getOrZero(fromId);
                if (value <= fromBalance) {
                    int toBalance = getOrZero(toId);
                    balances.put(fromId, fromBalance - value);
                    balances.put(toId, toBalance + value);
                }
                break;
            default:
                return false;
        }
        return false;
    }

    public static void writeFile(String filePath, Block block) {
        try
        {
            FileWriter fileWriter = new FileWriter(filePath);
            JsonFormat.printer().appendTo(block, fileWriter);
            fileWriter.close();
        }
        catch (IOException e)
        {
            System.out.println(e);
        }
    }
}
