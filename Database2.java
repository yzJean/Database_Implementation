import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.ArrayList;
import java.util.LinkedList;

public class Database2{
    private Row rows[] = new Row[100];

    @SuppressWarnings("unchecked")
    private LinkedBlockingQueue<Transaction>[] queues = (LinkedBlockingQueue<Transaction>[]) new LinkedBlockingQueue[10];

    private CountDownLatch latch;
    private List<Operation> sharedOperationList = new CopyOnWriteArrayList<>();
    private Lock listLock = new ReentrantLock();

    public Database2() {
        for (int i = 0; i < 100; i++) {
            rows[i] = new Row(i);
        }

        for (int i = 0; i < 10; i++) {
            queues[i] = new LinkedBlockingQueue<>();
        }
    }

    public void resetOperationList() {
        sharedOperationList.clear();
    }

    public void executeTransactions(List<Transaction> transactions) throws InterruptedException {
        latch = new CountDownLatch(10);

        // create threads for each partition
        List<Thread> threads = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            final int partitionId = i;
            Thread thread = new Thread(() -> processPartition(partitionId));
            threads.add(thread);
            thread.start();
        }

        for (Transaction t : transactions) {
            queues[0].put(t);
        }

        // add empty transaction to signal end of processing
        for (int i = 0; i < 10; i++) {
            queues[i].put(new Transaction());
        }

        // wait for all threads to complete
        latch.await();
    }

    private void processPartition(int partitionId) {
        try {
            while (true) {
                Transaction transaction = queues[partitionId].take();

                if (transaction.getOperations().isEmpty()) {
                    if (partitionId < 9) {
                        queues[partitionId + 1].put(transaction);
                    }
                    latch.countDown();
                    return;
                }

                // process operations in this partition
                List<Operation> processedOperations = new ArrayList<>();
                for (Operation o : transaction.getOperations()) {
                    int rowNum = o.getRowNumber();

                    // check if this partition is responsible for this row
                    if (rowNum >= partitionId * 10 && rowNum < (partitionId + 1) * 10) {
                        if (o.getType() == 0) { // read
                            System.out.println("Transaction "+o.getTransactionId()+" reads row " + rowNum + " = " + rows[rowNum].getValue());
                            o.setValue(rows[rowNum].getValue());
                        } else { // write
                            System.out.println("Transaction "+o.getTransactionId()+" writes row " + rowNum + " = " + o.getValue());
                            rows[rowNum].setValue(o.getValue());
                        }
                        processedOperations.add(o);

                        listLock.lock();
                        try {
                            sharedOperationList.add(o);
                        } finally {
                            listLock.unlock();
                        }
                    }
                }

                transaction.getOperations().removeAll(processedOperations);

                // pass transaction to next queue if not empty
                if (partitionId < 9 && !transaction.getOperations().isEmpty()) {
                    queues[partitionId + 1].put(transaction);
                }
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
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
        Database2 db = new Database2();
        ////////////////////////////////////////////////////////
        /// Run test cases
        ////////////////////////////////////////////////////////
        System.out.println("========== Test 1: test with transaction input 1 ==========");
        try {
            db.executeTransactions(transactionInput1());
        } catch (InterruptedException ex) {
            System.out.println(ex.getMessage());
        }
        verifySerializability(db.getLogHistory());
        db.resetOperationList(); // reset internal operation list for next test
        System.out.println();

        System.out.println("========== Test 2: test with transaction input 2 ==========");
        try {
            db.executeTransactions(transactionInput2());
        } catch (InterruptedException ex) {
            System.out.println(ex.getMessage());
        }
        verifySerializability(db.getLogHistory());
        db.resetOperationList(); // reset internal operation list for next test
        System.out.println();

        System.out.println("========== Test 3: test with transaction input 3 ==========");
        try {
            db.executeTransactions(transactionInput3());
        } catch (InterruptedException ex) {
            System.out.println(ex.getMessage());
        }
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
