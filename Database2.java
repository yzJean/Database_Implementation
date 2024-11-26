import java.util.List;
import java.util.LinkedList;

public class Database2{

    private Row rows[] = new Row[100];

    public Database2(){

	for(int i=0; i<100; i++){
           rows[i] = new Row(i);
        }
    }
     

    public void executeTransactions(List<Transaction> transactions){
        //Here I provide a serial implementation. You need to change it to a concurrent execution.
        for(Transaction t : transactions){
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

    public static void main(String []args){
	    Transaction t1 = new Transaction(1);
        t1.addOperation(new Operation(0, 3, 0, t1.getId())); // read
        t1.addOperation(new Operation(1, 4, 5, t1.getId())); // write
        
        Transaction t2 = new Transaction(2);
        t2.addOperation(new Operation(1, 3, 99, t2.getId())); // write
        t2.addOperation(new Operation(0, 4, 0, t2.getId())); // read

        LinkedList<Transaction> batch = new LinkedList<Transaction>();
        batch.add(t1);
        batch.add(t2);
        
        Database2 db = new Database2();
        db.executeTransactions(batch);
    }
}
