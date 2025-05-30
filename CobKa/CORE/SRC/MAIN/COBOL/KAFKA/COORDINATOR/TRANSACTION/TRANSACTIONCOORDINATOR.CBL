IDENTIFICATION DIVISION.
PROGRAM-ID. TRANSACTION-COORDINATOR.

ENVIRONMENT DIVISION.
CONFIGURATION SECTION.
REPOSITORY.
    COPY "TRANSACTION-CONFIG.cpy".
    COPY "TRANSACTION-STATE-MANAGER.cpy".
    COPY "TRANSACTION-MARKER-CHANNEL-MANAGER.cpy".
    COPY "PRODUCER-ID-MANAGER.cpy".

DATA DIVISION.
WORKING-STORAGE SECTION.
01 IS-ACTIVE PIC X(1) VALUE "N".
01 INIT-PRODUCER-ID-CALLBACK PIC X(1) VALUE "N".
01 ADD-PARTITIONS-CALLBACK PIC X(1) VALUE "N".
01 VERIFY-PARTITIONS-CALLBACK PIC X(1) VALUE "N".
01 END-TXN-CALLBACK PIC X(1) VALUE "N".
01 API-RESULT.
    05 ERRORS PIC X(10).
    05 RESULT PIC X(10).

PROCEDURE DIVISION.

MAIN-PROCEDURE.

    PERFORM STARTUP-PROCEDURE.
    PERFORM SHUTDOWN-PROCEDURE.

STARTUP-PROCEDURE.
    CALL "TRANSACTION-STATE-MANAGER-STARTUP"
        USING RETRIEVE-TRANSACTION-TOPIC-PARTITION-COUNT, ENABLE-TRANSACTIONAL-ID-EXPIRATION.
    CALL "TRANSACTION-MARKER-CHANNEL-MANAGER-START".
    MOVE "Y" TO IS-ACTIVE.
    DISPLAY "Startup complete.".

SHUTDOWN-PROCEDURE.
    MOVE "N" TO IS-ACTIVE.
    CALL "TRANSACTION-MARKER-CHANNEL-MANAGER-SHUTDOWN".
    CALL "TRANSACTION-STATE-MANAGER-SHUTDOWN".
    CALL "SCHEDULER-SHUTDOWN".
    DISPLAY "Shutdown complete.".

HANDLE-INIT-PRODUCER-ID.
    IF TRANSACTIONAL-ID IS NULL
        PERFORM GENERATE-NEW-PRODUCER-ID
    ELSE IF TRANSACTIONAL-ID IS EMPTY
        MOVE ERRORS-INVALID-REQUEST TO ERRORS
        PERFORM INIT-PRODUCER-ID-CALLBACK
    ELSE IF NOT VALIDATE-TRANSACTION-TIMEOUT-MS(TRANSACTION-TIMEOUT-MS)
        MOVE ERRORS-INVALID-TRANSACTION-TIMEOUT TO ERRORS
        PERFORM INIT-PRODUCER-ID-CALLBACK
    ELSE
        PERFORM GET-TRANSACTION-STATE
            GIVING COORDINATOR-EPOCH-AND-METADATA
        EVALUATE COORDINATOR-EPOCH-AND-METADATA
            WHEN NONE
                PERFORM CREATE-NEW-TRANSACTION-METADATA
                    GIVING COORDINATOR-EPOCH-AND-METADATA
            WHEN OTHER
                PERFORM PREPARE-INIT-PRODUCER-ID-TRANSIT
                    USING TRANSACTIONAL-ID, TRANSACTION-TIMEOUT-MS, COORDINATOR-EPOCH-AND-METADATA
                    GIVING API-RESULT
                EVALUATE API-RESULT
                    WHEN ERRORS NOT EQUAL ERRORS-NONE
                        MOVE ERRORS TO ERRORS
                        PERFORM INIT-PRODUCER-ID-CALLBACK
                    WHEN OTHER
                        PERFORM APPEND-TRANSACTION-TO-LOG
                        PERFORM INIT-PRODUCER-ID-CALLBACK
        END-EVALUATE
    END-IF.

HANDLE-LIST-TRANSACTIONS.
    IF NOT IS-ACTIVE
        MOVE ERRORS-COORDINATOR-NOT-AVAILABLE TO ERRORS
        PERFORM LIST-TRANSACTIONS-RESPONSE
    ELSE
        PERFORM LIST-TRANSACTION-STATES
            GIVING LIST-TRANSACTIONS-RESPONSE
    END-IF.

HANDLE-DESCRIBE-TRANSACTIONS.
    IF TRANSACTIONAL-ID IS NULL
        RAISE EXCEPTION "Invalid null transactionalId"
    ELSE IF TRANSACTIONAL-ID IS EMPTY
        MOVE ERRORS-INVALID-REQUEST TO ERRORS
        PERFORM DESCRIBE-TRANSACTIONS-RESPONSE
    ELSE
        PERFORM GET-TRANSACTION-STATE
            GIVING TRANSACTION-STATE
        EVALUATE TRANSACTION-STATE
            WHEN NONE
                MOVE ERRORS-TRANSACTIONAL-ID-NOT-FOUND TO ERRORS
                PERFORM DESCRIBE-TRANSACTIONS-RESPONSE
            WHEN OTHER
                PERFORM DESCRIBE-TRANSACTION-DETAILS
                    GIVING DESCRIBE-TRANSACTIONS-RESPONSE
        END-EVALUATE
    END-IF.

HANDLE-VERIFY-PARTITIONS-IN-TRANSACTION.
    IF TRANSACTIONAL-ID IS NULL OR TRANSACTIONAL-ID IS EMPTY
        MOVE ERRORS-INVALID-REQUEST TO ERRORS
        PERFORM VERIFY-PARTITIONS-CALLBACK
    ELSE
        PERFORM GET-TRANSACTION-STATE
            GIVING TRANSACTION-STATE
        EVALUATE TRANSACTION-STATE
            WHEN NONE
                MOVE ERRORS-INVALID-PRODUCER-ID-MAPPING TO ERRORS
                PERFORM VERIFY-PARTITIONS-CALLBACK
            WHEN OTHER
                PERFORM VERIFY-PARTITIONS-IN-TRANSACTION-DETAILS
                    GIVING VERIFY-PARTITIONS-CALLBACK
        END-EVALUATE
    END-IF.

HANDLE-ADD-PARTITIONS-TO-TRANSACTION.
    IF TRANSACTIONAL-ID IS NULL OR TRANSACTIONAL-ID IS EMPTY
        MOVE ERRORS-INVALID-REQUEST TO ERRORS
        PERFORM ADD-PARTITIONS-CALLBACK
    ELSE
        PERFORM GET-TRANSACTION-STATE
            GIVING TRANSACTION-STATE
        EVALUATE TRANSACTION-STATE
            WHEN NONE
                MOVE ERRORS-INVALID-PRODUCER-ID-MAPPING TO ERRORS
                PERFORM ADD-PARTITIONS-CALLBACK
            WHEN OTHER
                PERFORM ADD-PARTITIONS-TO-TRANSACTION-DETAILS
                    GIVING ADD-PARTITIONS-CALLBACK
        END-EVALUATE
    END-IF.

HANDLE-END-TRANSACTION.
    PERFORM END-TRANSACTION-DETAILS
        USING TRANSACTIONAL-ID, PRODUCER-ID, PRODUCER-EPOCH, TXN-MARKER-RESULT, IS-FROM-CLIENT, CLIENT-TXN-VERSION
        GIVING END-TXN-CALLBACK.

ON-ELECTION.
    DISPLAY "Elected as the txn coordinator for partition " TRANSACTION-TOPIC-PARTITION-ID " at epoch " COORDINATOR-EPOCH.
    PERFORM REMOVE-MARKERS-FOR-TXN-TOPIC-PARTITION USING TRANSACTION-TOPIC-PARTITION-ID.
    PERFORM LOAD-TRANSACTIONS-FOR-TXN-TOPIC-PARTITION USING TRANSACTION-TOPIC-PARTITION-ID, COORDINATOR-EPOCH, ADD-TXN-MARKERS-TO-SEND.

ON-RESIGNATION.
    DISPLAY "Resigned as the txn coordinator for partition " TRANSACTION-TOPIC-PARTITION-ID " at epoch " COORDINATOR-EPOCH.
    PERFORM REMOVE-TRANSACTIONS-FOR-TXN-TOPIC-PARTITION USING TRANSACTION-TOPIC-PARTITION-ID, COORDINATOR-EPOCH.
    PERFORM REMOVE-MARKERS-FOR-TXN-TOPIC-PARTITION USING TRANSACTION-TOPIC-PARTITION-ID.

PREPARE-INIT-PRODUCER-ID-TRANSIT.
    PERFORM LOCK-TRANSACTION-METADATA.
    EVALUATE TRANSACTION-METADATA-STATE
        WHEN PREPARE-ABORT, PREPARE-COMMIT
            MOVE ERRORS-CONCURRENT-TRANSACTIONS TO ERRORS
            RETURN
        WHEN COMPLETE-ABORT, COMPLETE-COMMIT, EMPTY
            PERFORM PREPARE-PRODUCER-ID-ROTATION
                USING PRODUCER-ID-MANAGER-GENERATE-PRODUCER-ID, TRANSACTION-TIMEOUT-MS, SYSTEM-CLOCK-MILLIS, EXPECTED-PRODUCER-ID-AND-EPOCH-DEFINED
                GIVING API-RESULT
            RETURN
        WHEN ONGOING
            MOVE TRANSACTION-METADATA-PREPARE-FENCE-PRODUCER-EPOCH TO API-RESULT
            RETURN
        WHEN DEAD, PREPARE-EPOCH-FENCE
            DISPLAY "Found transactionalId " TRANSACTIONAL-ID " with state " TRANSACTION-METADATA-STATE ". This is illegal as we should never have transitioned to this state."
            RAISE EXCEPTION "Invalid transaction state"
    END-EVALUATE.

END-TRANSACTION-DETAILS.
    EVALUATE CLIENT-TXN-VERSION
        WHEN TRANSACTION-VERSION-TV1
            PERFORM END-TRANSACTION-WITH-TV1
                USING TRANSACTIONAL-ID, PRODUCER-ID, PRODUCER-EPOCH, TXN-MARKER-RESULT, IS-FROM-CLIENT
                GIVING END-TXN-CALLBACK
        WHEN OTHER
            PERFORM END-TRANSACTION-WITH-TV2
                USING TRANSACTIONAL-ID, PRODUCER-ID, PRODUCER-EPOCH, TXN-MARKER-RESULT, IS-FROM-CLIENT, CLIENT-TXN-VERSION
                GIVING END-TXN-CALLBACK
    END-EVALUATE.

END-TRANSACTION-WITH-TV1.
    * Implement TV1 end transaction logic

END-TRANSACTION-WITH-TV2.
    * Implement TV2 end transaction logic

ABORT-TIMED-OUT-TRANSACTIONS.
    PERFORM TIMED-OUT-TRANSACTIONS
        GIVING TIMED-OUT-TXN-ID-AND-PID-EPOCH
    PERFORM VARYING TIMED-OUT-TXN-ID-AND-PID-EPOCH
        PERFORM GET-TRANSACTION-STATE
            GIVING TRANSACTION-STATE
        EVALUATE TRANSACTION-STATE
            WHEN NONE
                DISPLAY "Could not find transaction metadata when trying to timeout transaction for " TIMED-OUT-TXN-ID-AND-PID-EPOCH
            WHEN OTHER
                PERFORM LOCK-TRANSACTION-METADATA
                EVALUATE TRANSACTION-METADATA-PRODUCER-ID
                    WHEN TIMED-OUT-TXN-ID-AND-PID-EPOCH-PRODUCER-ID
                        IF NOT TRANSACTION-METADATA-PENDING-TRANSITION-IN-PROGRESS
                            MOVE TRANSACTION-METADATA-PREPARE-FENCE-PRODUCER-EPOCH TO TXN-TRANSIT-METADATA
                            PERFORM END-TRANSACTION
                                USING TRANSACTION-METADATA-TRANSACTIONAL-ID, TXN-TRANSIT-METADATA-PRODUCER-ID, TXN-TRANSIT-METADATA-PRODUCER-EPOCH, TRANSACTION-RESULT-ABORT, FALSE, TRANSACTION-VERSION-LEVEL, ON-END-TRANSACTION-COMPLETE(TIMED-OUT-TXN-ID-AND-PID-EPOCH)
                        ELSE
                            DISPLAY "Skipping abort of timed out transaction " TIMED-OUT-TXN-ID-AND-PID-EPOCH " since there is a pending state transition"
                    WHEN OTHER
                        DISPLAY "Found incorrect producerId when expiring transactionalId: " TIMED-OUT-TXN-ID-AND-PID-EPOCH-TRANSACTIONAL-ID ". Expected producerId: " TIMED-OUT-TXN-ID-AND-PID-EPOCH-PRODUCER-ID ". Found producerId: " TRANSACTION-METADATA-PRODUCER-ID
                END-EVALUATE
        END-EVALUATE
    END-PERFORM.

GENERATE-NEW-PRODUCER-ID.
    MOVE PRODUCER-ID-MANAGER-GENERATE-PRODUCER-ID TO PRODUCER-ID
    MOVE 0 TO PRODUCER-EPOCH
    MOVE ERRORS-NONE TO ERRORS
    PERFORM INIT-PRODUCER-ID-CALLBACK.

CREATE-NEW-TRANSACTION-METADATA.
    PERFORM CREATE-TRANSACTION-METADATA
        USING TRANSACTIONAL-ID, PRODUCER-ID-MANAGER-GENERATE-PRODUCER-ID, RECORD-BATCH-NO-PRODUCER-ID, RECORD-BATCH-NO-PRODUCER-ID, RECORD-BATCH-NO-PRODUCER-EPOCH, RECORD-BATCH-NO-PRODUCER-EPOCH, TRANSACTION-TIMEOUT-MS, EMPTY, EMPTY-SET, SYSTEM-CLOCK-MILLIS, TRANSACTION-VERSION-TV0
    PERFORM PUT-TRANSACTION-STATE-IF-NOT-EXISTS USING CREATED-TRANSACTION-METADATA
    RETURN COORDINATOR-EPOCH-AND-METADATA.

PROCEDURE DIVISION USING CONFIGURATION, REPLICA-MANAGER, SCHEDULER, CREATE-PRODUCER-ID-GENERATOR, METRICS, METADATA-CACHE, TIME.
    PERFORM INITIALIZE-TRANSACTION-CONFIG.
    PERFORM INITIALIZE-TRANSACTION-STATE-MANAGER.
    PERFORM INITIALIZE-TRANSACTION-MARKER-CHANNEL-MANAGER.
    MOVE "Y" TO IS-ACTIVE.
    MOVE PRODUCER-ID-MANAGER-INITIALIZE TO PRODUCER-ID-MANAGER.
    GOBACK.

INITIALIZE-TRANSACTION-CONFIG.
    MOVE CONFIG-TRANSACTIONAL-ID-EXPIRATION-MS TO TRANSACTION-CONFIG-TRANSACTIONAL-ID-EXPIRATION-MS.
    MOVE CONFIG-TRANSACTION-MAX-TIMEOUT-MS TO TRANSACTION-CONFIG-TRANSACTION-MAX-TIMEOUT-MS.
    MOVE CONFIG-TRANSACTION-TOPIC-PARTITIONS TO TRANSACTION-CONFIG-TRANSACTION-TOPIC-PARTITIONS.
    MOVE CONFIG-TRANSACTION-TOPIC-REPLICATION-FACTOR TO TRANSACTION-CONFIG-TRANSACTION-TOPIC-REPLICATION-FACTOR.
    MOVE CONFIG-TRANSACTION-TOPIC-SEGMENT-BYTES TO TRANSACTION-CONFIG-TRANSACTION-TOPIC-SEGMENT-BYTES.
    MOVE CONFIG-TRANSACTION-LOAD-BUFFER-SIZE TO TRANSACTION-CONFIG-TRANSACTION-LOAD-BUFFER-SIZE.
    MOVE CONFIG-TRANSACTION-TOPIC-MIN-ISR TO TRANSACTION-CONFIG-TRANSACTION-TOPIC-MIN-ISR.
    MOVE CONFIG-TRANSACTION-ABORT-TIMED-OUT-TRANSACTION-CLEANUP-INTERVAL-MS TO TRANSACTION-CONFIG-TRANSACTION-ABORT-TIMED-OUT-TRANSACTION-CLEANUP-INTERVAL-MS.
    MOVE CONFIG-TRANSACTION-REMOVE-EXPIRED-TRANSACTIONAL-ID-CLEANUP-INTERVAL-MS TO TRANSACTION-CONFIG-TRANSACTION-REMOVE-EXPIRED-TRANSACTIONAL-ID-CLEANUP-INTERVAL-MS.
    MOVE CONFIG-REQUEST-TIMEOUT-MS TO TRANSACTION-CONFIG-REQUEST-TIMEOUT-MS.

INITIALIZE-TRANSACTION-STATE-MANAGER.
    PERFORM INITIALIZE-TRANSACTION-STATE-MANAGER
        USING CONFIG-BROKER-ID, SCHEDULER, REPLICA-MANAGER, METADATA-CACHE, TRANSACTION-CONFIG, TIME, METRICS.

INITIALIZE-TRANSACTION-MARKER-CHANNEL-MANAGER.
    PERFORM INITIALIZE-TRANSACTION-MARKER-CHANNEL-MANAGER
        USING CONFIG, METRICS, METADATA-CACHE, TRANSACTION-STATE-MANAGER, TIME, LOG-CONTEXT.

LOCK-TRANSACTION-METADATA.
    PERFORM LOCK USING TRANSACTION-METADATA.

TIMED-OUT-TRANSACTIONS.
    PERFORM TRANSACTION-STATE-MANAGER-TIMED-OUT-TRANSACTIONS
        GIVING TIMED-OUT-TXN-ID-AND-PID-EPOCH.

GET-TRANSACTION-STATE.
    PERFORM TRANSACTION-STATE-MANAGER-GET-TRANSACTION-STATE USING TRANSACTIONAL-ID
        GIVING TRANSACTION-STATE.

APPEND-TRANSACTION-TO-LOG.
    PERFORM TRANSACTION-STATE-MANAGER-APPEND-TRANSACTION-TO-LOG
        USING TRANSACTIONAL-ID, COORDINATOR-EPOCH, TRANSACTION-METADATA, SEND-PID-RESPONSE-CALLBACK, REQUEST-LOCAL.

LIST-TRANSACTIONS-RESPONSE.
    PERFORM TRANSACTION-STATE-MANAGER-LIST-TRANSACTION-STATES
        USING FILTERED-PRODUCER-IDS, FILTERED-STATES, FILTERED-DURATION
        GIVING LIST-TRANSACTIONS-RESPONSE.

DESCRIBE-TRANSACTIONS-RESPONSE.
    PERFORM TRANSACTION-STATE-MANAGER-DESCRIBE-TRANSACTIONS
        USING TRANSACTIONAL-ID
        GIVING DESCRIBE-TRANSACTIONS-RESPONSE.

VERIFY-PARTITIONS-IN-TRANSACTION-DETAILS.
    PERFORM TRANSACTION-STATE-MANAGER-VERIFY-PARTITIONS-IN-TRANSACTION
        USING TRANSACTIONAL-ID, PRODUCER-ID, PRODUCER-EPOCH, PARTITIONS
        GIVING VERIFY-PARTITIONS-CALLBACK.

ADD-PARTITIONS-TO-TRANSACTION-DETAILS.
    PERFORM TRANSACTION-STATE