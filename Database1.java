import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Semaphore;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;

public class Database1 {

    private Row rows[] = new Row[100];
    private ReentrantReadWriteLock[] rowLocks = new ReentrantReadWriteLock[100];

    // shared list to store operations from all threads
    private List<Operation> sharedOperationList = new CopyOnWriteArrayList<>();
    private Lock listLock = new ReentrantLock();

    public Database1() {

        for (int i = 0; i < 100; i++) {
            rows[i] = new Row(i);
            rowLocks[i] = new ReentrantReadWriteLock();
        }
    }

    public void resetOperationList() {
        sharedOperationList.clear();
    }
    public void executeTransactions(List<Transaction> transactions) {
        CountDownLatch latch = new CountDownLatch(transactions.size());
        Semaphore[] semaphores = new Semaphore[transactions.size()];

        // initialize semaphores to control start order of transaction
        for (int i = 0; i < transactions.size(); i++) {
            semaphores[i] = new Semaphore(0);
        }
        semaphores[0].release();

        for (int i = 0; i < transactions.size(); i++) {
            Transaction t = transactions.get(i);
            Semaphore currentSemaphore = semaphores[i];
            Semaphore nextSemaphore = (i + 1 < transactions.size()) ? semaphores[i + 1] : null;

            Thread transactionThread = new Thread(() -> {

                // wait for permission to start
                try {
                    currentSemaphore.acquire();
                    // phase 1: acquire all locks in global order
                    List<Integer> lockOrder = t.getOperations().stream()
                            .map(Operation::getRowNumber)
                            .distinct()
                            .sorted()
                            .toList();

                    for (int rowNum : lockOrder) {
                        boolean isWrite = t.getOperations().stream()
                                .anyMatch(op -> op.getRowNumber() == rowNum && op.getType() == 1);
                        if (isWrite) {
                            rowLocks[rowNum].writeLock().lock();
                        } else {
                            rowLocks[rowNum].readLock().lock();
                        }
                    }

                    // signal the next transaction to start after aquire required locks
                    if (nextSemaphore != null) {
                        nextSemaphore.release();
                    }

                    // execute operations
                    for (Operation o : t.getOperations()) {
                        int rowNum = o.getRowNumber();
                        if (o.getType() == 0) { // read
                            System.out.println("Transaction " + t.getId() + " reads row " + rowNum + " = "
                                    + rows[rowNum].getValue());
                            o.setValue(rows[rowNum].getValue());
                        } else { // write
                            System.out.println(
                                    "Transaction " + t.getId() + " writes row " + rowNum + " = " + o.getValue());
                            rows[rowNum].setValue(o.getValue());
                        }

                        listLock.lock();
                        try {
                            sharedOperationList.add(o);
                        } finally {
                            listLock.unlock();
                        }

                    }

                    // signal completion of the transaction
                    latch.countDown();

                    // phase 2: release all locks
                    for (int rowNum : t.getOperations().stream().map(Operation::getRowNumber).distinct().sorted()
                            .toList()) {
                        boolean isWrite = t.getOperations().stream()
                                .anyMatch(op -> op.getRowNumber() == rowNum && op.getType() == 1);
                        if (isWrite) {
                            rowLocks[rowNum].writeLock().unlock();
                        } else {
                            rowLocks[rowNum].readLock().unlock();
                        }
                    }
                } catch (InterruptedException e) {
                    System.err.println("Transaction " + t.getId() + " interrupted: " + e.getMessage());
                }
            });
            transactionThread.start();
        }
        try {
            // wait for all transactions to complete
            latch.await();
        } catch (InterruptedException ex) {
            System.out.println(ex.getMessage());
        }
    }

    public List<Operation> getLogHistory() {
        return this.sharedOperationList;
    }

    public static LinkedList<Transaction> transactionInput1() {
        /*
         * Three transactions:
         * T1 = r1[x] r1[z] w1[x]
         * T2 = r2[y] r2[z] w2[y]
         * T3 = w3[x] r3[y] w3[z]
         */
        int x = 3; // row number
        int y = 4;
        int z = 5;
        Transaction t1 = new Transaction(1);
        t1.addOperation(new Operation(0, x, 0, t1.getId())); // read
        t1.addOperation(new Operation(0, z, 0, t1.getId())); // read
        t1.addOperation(new Operation(1, x, 5, t1.getId())); // write

        Transaction t2 = new Transaction(2);
        t2.addOperation(new Operation(0, y, 99, t2.getId())); // read
        t2.addOperation(new Operation(0, z, 0, t2.getId())); // read
        t2.addOperation(new Operation(1, y, 0, t2.getId())); // write

        Transaction t3 = new Transaction(3);
        t3.addOperation(new Operation(1, x, 99, t3.getId())); // write
        t3.addOperation(new Operation(0, y, 0, t3.getId())); // read
        t3.addOperation(new Operation(1, z, 0, t3.getId())); // write

        LinkedList<Transaction> batch2 = new LinkedList<Transaction>();
        batch2.add(t3);
        batch2.add(t2);
        batch2.add(t1);
        return batch2;
    }

    public static LinkedList<Transaction> transactionInput2() {
        /*
         * Three transactions:
         * T1 = w1[x] w1[x]
         * T2 = w2[x]
         */
        int x = 3; // row number
        Transaction t1 = new Transaction(1);
        t1.addOperation(new Operation(1, x, 0, t1.getId())); // write
        t1.addOperation(new Operation(1, x, 0, t1.getId())); // write

        Transaction t2 = new Transaction(2);
        t2.addOperation(new Operation(1, x, 99, t2.getId())); // write

        LinkedList<Transaction> batch = new LinkedList<Transaction>();
        batch.add(t2);
        batch.add(t1);
        return batch;
    }

    public static LinkedList<Transaction> transactionInput3() {
        Transaction t1 = new Transaction(1);
        t1.addOperation(new Operation(0, 3, 0, t1.getId())); // read
        t1.addOperation(new Operation(1, 4, 5, t1.getId())); // write

        Transaction t2 = new Transaction(2);
        t2.addOperation(new Operation(1, 3, 99, t2.getId())); // write
        t2.addOperation(new Operation(0, 4, 0, t2.getId())); // read

        LinkedList<Transaction> batch = new LinkedList<Transaction>();
        batch.add(t1);
        batch.add(t2);
        return batch;
    }

    public static void printLogInfo(List<Operation> ops) {
        System.out.print("Log information: ");
        for (Operation op : ops) {
            if (op.getType() == 0) {
                System.out.print("r" + op.getTransactionId() + " ");
            } else {
                System.out.print("w" + op.getTransactionId() + " ");
            }
        }
        System.out.println();
    }


    public static List<Operation> transactionLog1() {
        // test: cyclic
        int t1 = 1; // transactino id
        int t2 = 2;
        List<Operation> ops = new ArrayList<>();
        ops.add(new Operation(1, 3, 0, t1));
        ops.add(new Operation(1, 3, 5, t2));
        ops.add(new Operation(1, 3, 99, t1));
        printLogInfo(ops);
        return ops;
    }

    public static List<Operation> transactionLog2() {
        // test: non-cyclic
        int t1 = 1; // transactino id
        int t2 = 2;
        int t3 = 3;
        List<Operation> ops = new ArrayList<>();
        ops.add(new Operation(1, 1, 0, t3)); // w3[x]
        ops.add(new Operation(0, 1, 0, t1)); // r1[x]
        ops.add(new Operation(0, 2, 0, t3)); // r3[y]
        ops.add(new Operation(0, 2, 0, t2)); // r2[y]
        ops.add(new Operation(1, 3, 0, t3)); // w3[z]
        ops.add(new Operation(0, 3, 0, t2)); // r2[z]
        ops.add(new Operation(0, 3, 0, t1)); // r1[z]
        ops.add(new Operation(1, 2, 0, t2)); // w2[y]
        ops.add(new Operation(1, 1, 0, t1)); // w1[x]
        printLogInfo(ops);
        return ops;
    }

    public static void verifySerializability(List<Operation> ops) {
        SerializationGraph sg = new SerializationGraph(ops);
        if (sg.topologicalSort()) {
            sg.printTopologicalOrder();
        } else {
            System.out.println("This is not a serializable execution");
        }
    }

    public static void main(String[] args) {

        /* 
            // Example
            Transaction t1 = new Transaction(1);
            t1.addOperation(new Operation(0, 3, 0, t1.getId())); // read
            t1.addOperation(new Operation(1, 4, 5, t1.getId())); // write

            Transaction t2 = new Transaction(2);
            t2.addOperation(new Operation(1, 3, 99, t2.getId())); // write
            t2.addOperation(new Operation(0, 4, 0, t2.getId())); // read

            LinkedList<Transaction> batch1 = new LinkedList<Transaction>();
            batch1.add(t1);
            batch1.add(t2);
        */

        Database1 db = new Database1();

        ////////////////////////////////////////////////////////
        /// Run test cases
        ////////////////////////////////////////////////////////
        System.out.println("========== Test 1: test with transaction input 1 ==========");
        db.executeTransactions(transactionInput1());
        verifySerializability(db.getLogHistory());
        db.resetOperationList(); // reset internal operation list for next test
        System.out.println();

        System.out.println("========== Test 2: test with transaction input 2 ==========");
        db.executeTransactions(transactionInput2());
        verifySerializability(db.getLogHistory());
        db.resetOperationList(); // reset internal operation list for next test
        System.out.println();

        System.out.println("========== Test 3: test with transaction input 3 ==========");
        db.executeTransactions(transactionInput3());
        verifySerializability(db.getLogHistory());
        System.out.println();

        System.out.println("========== Test 4: test serialization graph functionality with cyclic log ==========");
        verifySerializability(transactionLog1());
        System.out.println();

        System.out.println("========== Test 5: test serialization graph functionality with non-cyclic log ==========");
        verifySerializability(transactionLog2());
        System.out.println();
    }
}
