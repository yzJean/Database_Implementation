
import java.util.*;

public class SerializationGraph {
    private List<Operation> log;
    private Map<Integer, Set<Integer>> adjList; // Adjacency list representation of the graph
    // List to store topological order
    private List<Integer> topologicalOrder = new ArrayList<>();

    public SerializationGraph(List<Operation> log) {
        this.adjList = new HashMap<>();
        this.log = log;

        buildGraph(log);
    }


    private void buildGraph(List<Operation> operations) {
        for (int i = 0; i < operations.size(); i++) {
            Operation o1 = operations.get(i);
            for (int j = i + 1; j < operations.size(); j++) {
                Operation o2 = operations.get(j);

                // Conflict conditions
                if (o1.getRowNumber() == o2.getRowNumber() &&
                        (o1.getType() == 1 || o2.getType() == 1) && // At least one is a write
                        o1.getTransactionId() != o2.getTransactionId()) { // Different transactions
                    // System.out.println("Op1 info, type: "+o1.getType()+", tId: "+o1.getTransactionId());
                    // System.out.println(" Op2 info, type: "+o2.getType()+", tId: "+o2.getTransactionId());
                    adjList.putIfAbsent(o1.getTransactionId(), new HashSet<>());
                    adjList.get(o1.getTransactionId()).add(o2.getTransactionId());
                }
            }
        }
    }

    public Boolean topologicalSort() {
        // Track incoming edges (in-degree) for each node
        Map<Integer, Integer> inDegree = new HashMap<>();
        
        // Initialize in-degree for all nodes
        for (Integer node : adjList.keySet()) {
            // System.out.println("adj list node info: "+node);
            inDegree.putIfAbsent(node, 0);
            for (Integer neighbor : adjList.get(node)) {
                // System.out.println(" adj list neighbor info: "+neighbor);
                inDegree.put(neighbor, inDegree.getOrDefault(neighbor, 0) + 1);
            }
        }

        // Queue for nodes with zero in-degree
        Queue<Integer> zeroInDegreeQueue = new LinkedList<>();
        for (Integer node : inDegree.keySet()) {
            if (inDegree.get(node) == 0) {
                System.out.println("node info: "+node);
                zeroInDegreeQueue.offer(node);
            }
        }
        if (zeroInDegreeQueue.isEmpty()) { // not serializable
            return false;
        }

        // Process nodes
        while (!zeroInDegreeQueue.isEmpty()) {
            Integer currentNode = zeroInDegreeQueue.poll();
            topologicalOrder.add(currentNode);
            
            // Reduce in-degree for neighbors
            if (adjList.containsKey(currentNode)) {
                for (Integer neighbor : adjList.get(currentNode)) {
                    inDegree.put(neighbor, inDegree.get(neighbor) - 1);
                    
                    // If in-degree becomes zero, add to queue
                    if (inDegree.get(neighbor) == 0) {
                        zeroInDegreeQueue.offer(neighbor);
                    }
                }
            }
        }

        return true;
    }

    // Optional method to print topological order
    public void printTopologicalOrder() {
        System.out.println("This execution is equivalent to a serial execution of:");
        for (int i = 0; i < topologicalOrder.size(); i++) {
            System.out.print("Transaction " + topologicalOrder.get(i));
            if (i < topologicalOrder.size() - 1) {
                System.out.print(" -> ");
            }
        }
        System.out.println();
    }

}
