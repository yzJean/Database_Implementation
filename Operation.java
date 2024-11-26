public class Operation{
    private int type; //0 for READ and 1 for WRITE
    private int rowNumber; //which row to read or write
    private int value; //for read, this is the return value; for write, this is the value to write;
    private int tId; // transaction id. TODO: refactor

    public Operation(int type, int rowNumber, int value, int tId){
        this.type = type;
        this.rowNumber = rowNumber;
        this.value = value;
        this.tId = tId;
    }

    public int getType(){
        return this.type;
    }

    public int getRowNumber(){
        return this.rowNumber;
    }

    public int getValue(){
        return this.value;
    }

    public int getTransactionId(){
        return this.tId;
    }

    public void setValue(int value){
        this.value = value;
    }

    public String toString(){
	if(type == 0)
             return "READ row "+rowNumber+" value "+value;
        else
             return "WRITE row "+rowNumber+" value "+value;	
    }

}
