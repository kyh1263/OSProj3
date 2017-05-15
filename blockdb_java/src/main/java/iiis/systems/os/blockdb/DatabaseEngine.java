package iiis.systems.os.blockdb;

import java.util.HashMap;
import java.util.regex.*;
import java.io.*;
import com.google.protobuf.util.JsonFormat;

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
    final String template = "[a-z0-9]{" + userIdLength + "}";
    Pattern pattern = Pattern.compile(template, Pattern.CASE_INSENSITIVE);
    Block.Builder blockBuilder = Block.newBuilder();

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
        if (pattern.matcher(userId).matches() && value>=0)
        {
            if (logLength % N == 0)
                newBlock(++blockId);
            Transaction transaction = Transaction.newBuilder().setType(Transaction.Types.PUT).setUserID(userId).setValue(value).build();
            blockBuilder.addTransactions(transaction);
            writeFile("./log/" + blockId + ".json", blockBuilder.build().toString());//JsonFormat.printToString(blockBuilder.build());
            logLength++;
            balances.put(userId, value);
            return true;
        }
        else return false;
    }

    public boolean deposit(String userId, int value) {
        if (pattern.matcher(userId).matches() && value>=0)
        {
            if (logLength % N == 0)
                newBlock(++blockId);
            Transaction transaction = Transaction.newBuilder().setType(Transaction.Types.DEPOSIT).setUserID(userId).setValue(value).build();
            blockBuilder.addTransactions(transaction);
            writeFile("./log/" + blockId + ".json", blockBuilder.build().toString());//JsonFormat.printToString(blockBuilder.build());
            logLength++;
            int balance = getOrZero(userId);
            balances.put(userId, balance + value);
            return true;
        }
        else return false;
    }

    public boolean withdraw(String userId, int value) {
        if (pattern.matcher(userId).matches() && value>=0)
        {
            int balance = getOrZero(userId);
            if (value<=balance)
            {
                if (logLength % N == 0)
                    newBlock(++blockId);
                Transaction transaction = Transaction.newBuilder().setType(Transaction.Types.WITHDRAW).setUserID(userId).setValue(value).build();
                blockBuilder.addTransactions(transaction);
                writeFile("./log/" + blockId + ".json", blockBuilder.build().toString());//JsonFormat.printToString(blockBuilder.build());
                logLength++;
                balances.put(userId, balance - value);
                return true;
            }
        }
        return false;
    }

    public boolean transfer(String fromId, String toId, int value) {
        if (pattern.matcher(fromId).matches() && pattern.matcher(toId).matches() && value>=0)
        {
            int fromBalance = getOrZero(fromId);
            if (value<=fromBalance)
            {
                if (logLength % N == 0)
                    newBlock(++blockId);
                Transaction transaction = Transaction.newBuilder().setType(Transaction.Types.TRANSFER).setFromID(fromId).setToID(toId).setValue(value).build();
                blockBuilder.addTransactions(transaction);
                writeFile("./log/" + blockId + ".json", blockBuilder.build().toString());//JsonFormat.printToString(blockBuilder.build());
                logLength++;
                int toBalance = getOrZero(toId);
                balances.put(fromId, fromBalance - value);
                balances.put(toId, toBalance + value);
                return true;
            }
        }
        return false;
    }

    public int getLogLength() {
        return logLength;
    }

    public void newBlock(int blockId) {
        blockBuilder.setBlockID(blockId).setPrevHash("00000000").clearTransactions().setNonce("00000000");
    }

    public static void writeFile(String filePath, String string) {
        try {
            FileWriter fileWriter = new FileWriter(filePath);
            PrintWriter printWriter = new PrintWriter(fileWriter);
            printWriter.write(string);
            printWriter.println();
            fileWriter.close();
            printWriter.close();
        } catch (IOException e) {};
    }
}
