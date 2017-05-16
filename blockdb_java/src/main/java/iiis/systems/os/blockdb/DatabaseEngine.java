package iiis.systems.os.blockdb;

import java.util.HashMap;
import java.util.concurrent.Semaphore;
import java.util.regex.*;
import java.io.*;
import com.google.protobuf.util.JsonFormat;
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
    private int logLength = 0;
    private String dataDir;
    private int blockId = 0;
    final int N = 3/*50*/;
    final int userIdLength = 8;
    final String template = "[a-z0-9|-]{" + userIdLength + "}";
    Pattern pattern = Pattern.compile(template, Pattern.CASE_INSENSITIVE);
    Block.Builder blockBuilder = Block.newBuilder();
    String serverLogInfoPath = dataDir + "serverLogInfo.json";

    private Semaphore semaphore = new Semaphore(1);

    DatabaseEngine(String dataDir) {
        this.dataDir = dataDir;
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
        return getOrZero(userId);
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
        return logLength;
    }

    public void writeTransactionLog(Transaction transaction) {
        if (logLength == 0)
            blockBuilder.setBlockID(++blockId).setPrevHash("00000000").clearTransactions().setNonce("00000000");
        blockBuilder.addTransactions(transaction);
        writeFile(dataDir + blockId + ".json", blockBuilder.build());
        logLength++;
        logLength%=N;
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
                    Transaction thisTransaction = builder.getTransactions(j);
                    String userId = thisTransaction.getUserID();
                    String fromId = thisTransaction.getFromID();
                    String toId = thisTransaction.getToID();
                    int value = thisTransaction.getValue();
                    Transaction.Types type = thisTransaction.getType();
                    int balance = 0;

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
                }

            }

            //restore transient logs
            int i = completeBlocks + 1;
            String jsonPath = dataDir + i + ".json";
            JSONObject jsonblock = Util.readJsonFile(jsonPath);

            semaphore.release();
            return true;
        } catch (Exception e) {
            e.printStackTrace();
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
