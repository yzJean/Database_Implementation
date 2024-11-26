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

    public void clearOperationList() {
        sharedOperationList.clear();
    }

    public void executeTransactionsSerial(List<Transaction> transactions) {
        // Here I provide a serial implementation. You need to change it to a concurrent
        // execution.
        for (Transaction t : transactions) {
            System.out.println("T" + t.getId());

            // Sorting operations by rowNumber
            Collections.sort(t.getOperations(), (o1, o2) -> Integer.compare(o1.getRowNumber(), o2.getRowNumber()));

            for (Operation o : t.getOperations()) {
                System.out.println("executing " + o);
                if (o.getType() == 0) { // read
                    o.setValue(rows[o.getRowNumber()].getValue());
                } else { // write
                    rows[o.getRowNumber()].setValue(o.getValue());
                }
                sharedOperationList.add(o);
            }
        }
    }

    public void executeTransactionsConcurrent(List<Transaction> transactions) {
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
                    // phase 1: Acquire all locks in global order
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

                    // Simulate commit phase by delaying lock release until transaction completion
                    System.out.println("T" + t.getId() + " committing");
                    latch.countDown(); // Signal completion of the transaction

                    // Second phase: Release all locks
                    System.out.println("T" + t.getId() + " releases all locks");
                    for (int rowNum : t.getOperations().stream().map(Operation::getRowNumber).distinct().sorted()
                            .toList()) {
                        boolean isWrite = t.getOperations().stream()
                                .anyMatch(op -> op.getRowNumber() == rowNum && op.getType() == 1);
                        if (isWrite) {
                            System.out.println(" T" + t.getId() + " releases read lock at row " + rowNum);
                            rowLocks[rowNum].writeLock().unlock();
                        } else {
                            System.out.println(" T" + t.getId() + " releases write lock at row " + rowNum);
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
            // Wait for all transactions to complete
            latch.await();
        } catch (InterruptedException ex) {
            System.out.println(ex.getMessage());
        }
        // Print out the shared operation list
        System.out.println("Shared Operation List:");
        for (Operation op : sharedOperationList) {
            if (op.getType() == 0) {
                System.out.println(" T" + op.getTransactionId() + " reads at row" + op.getRowNumber());
            } else {
                System.out.println(" T" + op.getTransactionId() + " writes at  row" + op.getRowNumber());
            }
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
        int x = 3; // row
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
        batch.add(t1);
        batch.add(t2);
        return batch;
    }

    public static List<Operation> transactionLog1() {
        // test: cyclic
        int t1 = 1;
        int t2 = 2;
        List<Operation> ops = new ArrayList<>();
        ops.add(new Operation(1, 3, 0, t1));
        ops.add(new Operation(1, 3, 5, t2));
        ops.add(new Operation(1, 3, 99, t1));
        return ops;
    }

    public static List<Operation> transactionLog2() {
        // test: non-cyclic
        int t1 = 1;
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


        Database1 dbSerial = new Database1();
        Database1 dbConcurrent = new Database1();

        ////////////////////////////////////////////////////////
        /// Run test cases
        ////////////////////////////////////////////////////////
        System.out.println("========== Test 1 ==========");
        System.out.println("Serial: ");
        dbSerial.executeTransactionsSerial(transactionInput1());
        verifySerializability(dbSerial.getLogHistory());


        System.out.println("Concurrent: ");
        dbConcurrent.executeTransactionsConcurrent(transactionInput1());
        verifySerializability(dbConcurrent.getLogHistory());

        dbSerial.clearOperationList();
        dbConcurrent.clearOperationList();

        System.out.println("========== Test 2 ==========");
        System.out.println("Serial: ");
        dbSerial.executeTransactionsSerial(transactionInput2());
        verifySerializability(dbSerial.getLogHistory());


        System.out.println("Concurrent: ");
        dbConcurrent.executeTransactionsConcurrent(transactionInput2());
        verifySerializability(dbConcurrent.getLogHistory());

        System.out.println("========== Test 3: test with cyclic log ==========");
        verifySerializability(transactionLog1());

        System.out.println("========== Test 4: test with non-cyclic log ==========");
        verifySerializability(transactionLog2());
    }
}
