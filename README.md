# Lab3: Database Implementation
## Overview
This project implements a system for analyzing and managing transactional operations in a database. The system comprises three main components:

    1. Database1: Simulates transactions using a two-phase locking protocol to ensure isolation.
    2. Database2: Implements a partitioned approach for processing transactions.
    3. SerializationGraph: Constructs a serialization graph to analyze transaction execution for serializability.
The system is designed to demonstrate the functionality of concurrent transaction execution, conflict detection, and serialization testing.

## Key Implementations
* Paradigm 1: Strict Two-Phase Locking (`Database1.java`)
    * Ensured that all locks are acquired in a consistent global order to prevent deadlocks
    * Handled both read and write operations, locking rows accordingly
    * Integrated with `SerializationGraph` to analyze whether transaction logs are serializable

* Paradigm 2: Pipelined Execution (`Database2.java`)
    *  Divided rows into 10 partitions and assigns a queue and thread for each partition. (0-9, 10-19, etc.)
    * Transfered transactions between partitions when necessary
    * No explicit synchronization needed as each thread handles distinct rows

* `SerializationGraph.java`
    * Conflict Graph Construction: Identifies conflicts between transactions based on shared rows and operation types.
    * Topological Sorting: Uses in-degree counting and a queue to detect cycles and find a serializable order if possible.
    * Readable Outputs: Provides a clear representation of the execution order or reports non-serializability.

## Running the Program
Two ways to run the program: run an all-in-one script, or run manually

* Option 1:
    Run the following script:
    ```
    $ cd Lab3
    $ start.sh
    ```

* Option 2:
    Manually compile and run
    1.  Build
        ```
        javac *.java
        ```
    2.  Run
        * Paradigm 1: Strict Two-Phase Locking
        ```
        java Database1
        ```

        * Paradigm 2: Pipelined Execution
        ```
        java Database2
        ```

## Example Output
* From `Database1`
    ```
        ========== Test 1: test with transaction input 1 ==========
        Transaction 3 writes row 3 = 99
        Transaction 3 reads row 4 = 4
        ...
        This execution is equivalent to a serial execution of:
        Transaction 3 -> Transaction 2 -> Transaction 1
    ```

* From `Database2`
    ```
        ========== Test 3: test with transaction input 3 ==========
        Transaction 3 writes row 3 = 99
        Transaction 3 reads row 4 = 4
        ...
        This is not a serializable execution
    ```

## Future works
* Refactor `Database1.java`, `Database2.java`, and `SerializationGraph.java` for readibility, extensibility, and maintainablity
* Test programs with more test cases