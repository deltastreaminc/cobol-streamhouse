IDENTIFICATION DIVISION.
PROGRAM-ID. DELAYED-REMOTE-FETCH.

ENVIRONMENT DIVISION.
CONFIGURATION SECTION.
REPOSITORY.
    COPY KAFKA-METRICS.

DATA DIVISION.
WORKING-STORAGE SECTION.
01 WS-REMOTE-FETCH-TASK             USAGE IS POINTER.
01 WS-REMOTE-FETCH-RESULT           USAGE IS POINTER.
01 WS-REMOTE-FETCH-INFO             USAGE IS POINTER.
01 WS-REMOTE-FETCH-MAX-WAIT-MS      PIC 9(18) COMP-3.
01 WS-FETCH-PARTITION-STATUS        USAGE IS POINTER.
01 WS-FETCH-PARAMS                  USAGE IS POINTER.
01 WS-LOCAL-READ-RESULTS            USAGE IS POINTER.
01 WS-REPLICA-MANAGER               USAGE IS POINTER.
01 WS-RESPONSE-CALLBACK             USAGE IS POINTER.

01 WS-TOPIC-PARTITION               USAGE IS POINTER.
01 WS-FETCH-STATUS                  USAGE IS POINTER.
01 WS-FETCH-OFFSET                  PIC 9(18) COMP-3.

01 WS-CANCELLED                     PIC X(1).

PROCEDURE DIVISION.

MAIN-PARAGRAPH.
    IF WS-FETCH-PARAMS-IS-FROM-FOLLOWER
        RAISE ILLEGAL-STATE-EXCEPTION
            WITH DATA 'The follower should not invoke remote fetch. Fetch params are: ' WS-FETCH-PARAMS.

    PERFORM UNTIL WS-REMOTE-FETCH-RESULT-IS-DONE OR
            WS-FETCH-PARTITION-STATUS-PROCESSED
        PERFORM VARYING WS-FETCH-PARTITION-STATUS-IDX
            FROM 1 BY 1 UNTIL WS-FETCH-PARTITION-STATUS-IDX > WS-FETCH-PARTITION-STATUS-COUNT
            MOVE WS-FETCH-PARTITION-STATUS-IDX TO WS-TOPIC-PARTITION
            MOVE WS-FETCH-PARTITION-STATUS-IDX TO WS-FETCH-STATUS
            PERFORM CHECK-PARTITION-STATUS
        END-PERFORM
    END-PERFORM.

    IF WS-REMOTE-FETCH-RESULT-IS-DONE
        PERFORM FORCE-COMPLETE.

    PERFORM ON-EXPIRATION.
    PERFORM ON-COMPLETE.

CHECK-PARTITION-STATUS.
    MOVE WS-FETCH-OFFSET TO WS-FETCH-OFFSET-METADATA.
    IF WS-FETCH-OFFSET-METADATA NOT = UNKNOWN-OFFSET-METADATA
        CALL 'GET-PARTITION-OR-EXCEPTION'
            USING WS-TOPIC-PARTITION
            RETURNING WS-REPLICA-MANAGER-PARTITION
    ELSE
        PERFORM HANDLE-OFFLINE-LOG-DIRECTORY
        PERFORM FORCE-COMPLETE
        GO TO CHECK-PARTITION-STATUS-EXIT
    END-IF.

    PERFORM HANDLE-UNKNOWN-TOPIC-OR-PARTITION
    PERFORM HANDLE-NOT-LEADER-OR-FOLLOWER
    GO TO CHECK-PARTITION-STATUS-EXIT.

HANDLE-OFFLINE-LOG-DIRECTORY.
    DISPLAY 'Partition ' WS-TOPIC-PARTITION ' is in an offline log directory, satisfy ' WS-FETCH-PARAMS ' immediately'
    RETURN.

HANDLE-UNKNOWN-TOPIC-OR-PARTITION.
    DISPLAY 'Broker no longer knows of partition ' WS-TOPIC-PARTITION ', satisfy ' WS-FETCH-PARAMS ' immediately'
    PERFORM FORCE-COMPLETE
    RETURN.

HANDLE-NOT-LEADER-OR-FOLLOWER.
    DISPLAY 'Broker is no longer the leader or follower of ' WS-TOPIC-PARTITION ', satisfy ' WS-FETCH-PARAMS ' immediately'
    PERFORM FORCE-COMPLETE
    RETURN.

CHECK-PARTITION-STATUS-EXIT.
    EXIT.

ON-EXPIRATION.
    MOVE 'Y' TO WS-CANCELLED.
    CALL 'CANCEL-REMOTE-FETCH-TASK'
        USING WS-REMOTE-FETCH-TASK
        RETURNING WS-CANCELLED.
    IF WS-CANCELLED = 'N'
        DISPLAY 'Remote fetch task for RemoteStorageFetchInfo: ' WS-REMOTE-FETCH-INFO ' could not be cancelled and its isDone value is ' WS-REMOTE-FETCH-TASK-IS-DONE
    END-IF.
    CALL 'MARK-EXPIRED-REQUEST-METER'.

FORCE-COMPLETE.
    MOVE 'Y' TO WS-FETCH-PARTITION-STATUS-PROCESSED.
    RETURN.

ON-COMPLETE.
    PERFORM VARYING WS-LOCAL-READ-RESULTS-IDX
        FROM 1 BY 1 UNTIL WS-LOCAL-READ-RESULTS-IDX > WS-LOCAL-READ-RESULTS-COUNT
        MOVE WS-LOCAL-READ-RESULTS-IDX TO WS-TOPIC-PARTITION
        IF WS-TOPIC-PARTITION = WS-REMOTE-FETCH-INFO-TOPIC-PARTITION
            AND WS-REMOTE-FETCH-RESULT-IS-DONE
            AND WS-LOCAL-READ-RESULT-ERROR = NONE
            AND WS-LOCAL-READ-RESULT-DELAYED-REMOTE-STORAGE-FETCH-PRESENT
            IF WS-REMOTE-FETCH-RESULT-ERROR-PRESENT
                MOVE WS-REMOTE-FETCH-RESULT-ERROR TO WS-LOCAL-READ-RESULT-ERROR
                CALL 'CREATE-LOG-READ-RESULT'
                    USING WS-REMOTE-FETCH-RESULT-ERROR
                    RETURNING WS-LOCAL-READ-RESULT
            ELSE
                MOVE WS-REMOTE-FETCH-RESULT-FETCH-DATA-INFO TO WS-LOCAL-READ-RESULT-FETCH-PARTITION-DATA
                CALL 'CREATE-FETCH-PARTITION-DATA'
                    USING WS-LOCAL-READ-RESULT-ERROR
                         WS-LOCAL-READ-RESULT-HIGH-WATERMARK
                         WS-LOCAL-READ-RESULT-LEADER-LOG-START-OFFSET
                         WS-REMOTE-FETCH-RESULT-FETCH-DATA-INFO-RECORDS
                         WS-REMOTE-FETCH-RESULT-FETCH-DATA-INFO-ABORTED-TRANSACTIONS
                         WS-LOCAL-READ-RESULT-LAST-STABLE-OFFSET
                         WS-LOCAL-READ-RESULT-PREFERRED-READ-REPLICA
                         WS-REMOTE-FETCH-RESULT-FETCH-DATA-INFO-ERROR
                    RETURNING WS-LOCAL-READ-RESULT-FETCH-PARTITION-DATA
            END-IF
        ELSE
            CALL 'CREATE-FETCH-PARTITION-DATA'
                USING WS-LOCAL-READ-RESULT-ERROR
                     WS-LOCAL-READ-RESULT-HIGH-WATERMARK
                     WS-LOCAL-READ-RESULT-LEADER-LOG-START-OFFSET
                     WS-LOCAL-READ-RESULT-RECORDS
                     WS-LOCAL-READ-RESULT-ABORTED-TRANSACTIONS
                     WS-LOCAL-READ-RESULT-LAST-STABLE-OFFSET
                     WS-LOCAL-READ-RESULT-PREFERRED-READ-REPLICA
                     WS-LOCAL-READ-RESULT-IS-FROM-FOLLOWER
                RETURNING WS-LOCAL-READ-RESULT-FETCH-PARTITION-DATA
        END-IF
        CALL 'RESPONSE-CALLBACK'
            USING WS-TOPIC-PARTITION
                 WS-LOCAL-READ-RESULT-FETCH-PARTITION-DATA
    END-PERFORM.

STOP RUN.