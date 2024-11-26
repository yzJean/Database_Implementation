import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.ArrayList;
import java.util.LinkedList;

public class Database1{

    private Row rows[] = new Row[100];
    private ReentrantReadWriteLock[] rowLocks = new ReentrantReadWriteLock[100];

    // shared list to store operations from all threads
    private List<Operation> sharedOperationList = new CopyOnWriteArrayList<>();
    private Lock listLock = new ReentrantLock();

    public Database1(){

	for(int i=0; i<100; i++){
           rows[i] = new Row(i);
           rowLocks[i] = new ReentrantReadWriteLock();
        }
    }
     

    public void executeTransactionsSerial(List<Transaction> transactions){
        //Here I provide a serial implementation. You need to change it to a concurrent execution.
        for(Transaction t : transactions){
            System.out.println("T"+t.getId());
            for(Operation o : t.getOperations()){
                System.out.println("executing "+o);
                if(o.getType()==0){ // read
                   o.setValue(rows[o.getRowNumber()].getValue());
                }
                else{ // write
                   rows[o.getRowNumber()].setValue(o.getValue());
                }
            }
        }
    }

    public void executeTransactionsConcurrent(List<Transaction> transactions) {
        List<Thread> transactionThreads = new ArrayList<>();
        CountDownLatch latch = new CountDownLatch(transactions.size());

        for (Transaction t : transactions) {
            Thread transactionThread = new Thread(() -> {
                // First phase: Acquire all locks
                System.out.println("T"+t.getId()+" acquire all locks");
                for (Operation o : t.getOperations()) {
                    int rowNum = o.getRowNumber();
                    if (o.getType() == 0) { // Read
                        rowLocks[rowNum].readLock().lock();
                    } else { // Write
                        rowLocks[rowNum].writeLock().lock();
                    }
                }

                // Execute operations
                System.out.println("T"+t.getId()+" excuting");
                for (Operation o : t.getOperations()) {
                    int rowNum = o.getRowNumber();
                    if (o.getType() == 0) { // Read
                        System.out.println("Transaction reads row " + rowNum + " = " + rows[rowNum].getValue());
                        o.setValue(rows[rowNum].getValue());
                    } else { // Write
                        System.out.println("Transaction writes row " + rowNum + " = " + o.getValue());
                        rows[rowNum].setValue(o.getValue());
                    }

                    // Safely add write operation to shared list
                    listLock.lock();
                    try {
                        sharedOperationList.add(o);
                    } finally {
                        listLock.unlock();
                    }

                }

                // Second phase: Release all locks
                System.out.println("T"+t.getId()+" releases all locks");
                for (Operation o : t.getOperations()) {
                    int rowNum = o.getRowNumber();
                    if (o.getType() == 0) { // Read
                        rowLocks[rowNum].readLock().unlock();
                    } else { // Write
                        rowLocks[rowNum].writeLock().unlock();
                    }
                }

                latch.countDown();
            });

            transactionThreads.add(transactionThread);
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
            System.out.println(op);
        }
    }

    public List<Operation> getLogHistory() {
        return this.sharedOperationList;
    }

    public static void main(String []args){
        /*
        Example
	    Transaction t1 = new Transaction(1);
        t1.addOperation(new Operation(0, 3, 0, t1.getId())); // read
        t1.addOperation(new Operation(1, 4, 5, t1.getId())); // write
        
        Transaction t2 = new Transaction(2);
        t2.addOperation(new Operation(1, 3, 99, t2.getId())); // write
        t2.addOperation(new Operation(0, 4, 0, t2.getId())); // read
        */

        Transaction t1 = new Transaction(1);
        t1.addOperation(new Operation(0, 3, 0, t1.getId())); // read
        t1.addOperation(new Operation(1, 4, 5, t1.getId())); // write
        
        Transaction t2 = new Transaction(2);
        t2.addOperation(new Operation(1, 3, 99, t2.getId())); // write
        t2.addOperation(new Operation(0, 4, 0, t2.getId())); // read


        LinkedList<Transaction> batch = new LinkedList<Transaction>();
        batch.add(t1);
        batch.add(t2);
        
        Database1 dbSerial = new Database1();
        System.out.println("Serial: ");
        dbSerial.executeTransactionsSerial(batch);

        Database1 dbConcurrent = new Database1();
        System.out.println("Concurrent: ");
        dbConcurrent.executeTransactionsConcurrent(batch);

        // // test: cyclic
        // List<Operation> ops = new ArrayList<>();
        // System.out.println(t1.getId());
        // ops.add(new Operation(1, 3, 0, t1.getId()));
        // ops.add(new Operation(1, 3, 5, t2.getId()));
        // ops.add(new Operation(1, 3, 99, t1.getId()));
        Transaction t3 = new Transaction(3); // T3 = w3[x] r3[y] w3[z]

        // Define operations for transactions based on logs
        // test: non-cyclic
        List<Operation> ops = new ArrayList<>();
        ops.add(new Operation(1, 1, 0, t3.getId())); // w3[x]
        ops.add(new Operation(0, 1, 0, t1.getId())); // r1[x]
        ops.add(new Operation(0, 2, 0, t3.getId())); // r3[y]
        ops.add(new Operation(0, 2, 0, t2.getId())); // r2[y]
        ops.add(new Operation(1, 3, 0, t3.getId())); // w3[z]
        ops.add(new Operation(0, 3, 0, t2.getId())); // r2[z]
        ops.add(new Operation(0, 3, 0, t1.getId())); // r1[z]
        ops.add(new Operation(1, 2, 0, t2.getId())); // w2[y]
        ops.add(new Operation(1, 1, 0, t1.getId())); // w1[x]

        // System.out.println(t1.getId());
        // ops.add(new Operation(1, 3, 0, t1.getId()));
        // ops.add(new Operation(1, 3, 5, t2.getId()));
        // ops.add(new Operation(1, 3, 99, t1.getId()));

        // SerializationGraph sg = new SerializationGraph(dbConcurrent.getLogHistory());
        SerializationGraph sg = new SerializationGraph(ops);
        if (sg.topologicalSort()) {
            sg.printTopologicalOrder();
        } else {
            System.out.println("Not serializable");
        }
    }
}
