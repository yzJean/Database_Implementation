import java.util.List;
import java.util.LinkedList;

public class Transaction{
    private LinkedList<Operation> operations;
    private int id; // transaction id

    public Transaction(){
        this.operations = new LinkedList<Operation>();
        this.id = 0;
    }

    public Transaction(int id){
        this.operations = new LinkedList<Operation>();
        this.id = id;
    }
    
    public void addOperation(Operation o){
        operations.add(o);
    }

    public List<Operation> getOperations(){
        return this.operations;
    }

    public int getId(){
        return this.id;
    }
} 
