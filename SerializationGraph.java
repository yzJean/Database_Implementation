
import java.util.*;

public class SerializationGraph {
    private Map<Integer, Set<Integer>> adjList;
    private List<Integer> topologicalOrder = new ArrayList<>();

    public SerializationGraph(List<Operation> log) {
        this.adjList = new HashMap<>();
        buildGraph(log);
    }

    private void buildGraph(List<Operation> operations) {
        for (int i = 0; i < operations.size(); i++) {
            Operation o1 = operations.get(i);
            for (int j = i + 1; j < operations.size(); j++) {
                Operation o2 = operations.get(j);

                // conflict conditions
                if (o1.getRowNumber() == o2.getRowNumber() &&
                        (o1.getType() == 1 || o2.getType() == 1) &&
                        o1.getTransactionId() != o2.getTransactionId()) {
                    adjList.putIfAbsent(o1.getTransactionId(), new HashSet<>());
                    adjList.get(o1.getTransactionId()).add(o2.getTransactionId());
                }
            }
        }
    }

    public Boolean topologicalSort() {
        Map<Integer, Integer> inDegree = new HashMap<>();

        for (Integer node : adjList.keySet()) {
            inDegree.putIfAbsent(node, 0);
            for (Integer neighbor : adjList.get(node)) {
                inDegree.put(neighbor, inDegree.getOrDefault(neighbor, 0) + 1);
            }
        }

        Queue<Integer> zeroInDegreeQueue = new LinkedList<>();
        for (Integer node : inDegree.keySet()) {
            if (inDegree.get(node) == 0) {
                zeroInDegreeQueue.offer(node);
            }
        }

        // If it has a cycle, zeroInDegreeQueue is empty
        if (zeroInDegreeQueue.isEmpty()) {
            return false;
        }

        while (!zeroInDegreeQueue.isEmpty()) {
            Integer currentNode = zeroInDegreeQueue.poll();
            topologicalOrder.add(currentNode);

            if (adjList.containsKey(currentNode)) {
                for (Integer neighbor : adjList.get(currentNode)) {
                    inDegree.put(neighbor, inDegree.get(neighbor) - 1);
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
