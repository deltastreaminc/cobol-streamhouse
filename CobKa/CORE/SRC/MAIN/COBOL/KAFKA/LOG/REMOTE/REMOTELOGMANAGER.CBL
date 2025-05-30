IDENTIFICATION DIVISION.
PROGRAM-ID. REMOTE-LOG-MANAGER.

ENVIRONMENT DIVISION.
CONFIGURATION SECTION.
INPUT-OUTPUT SECTION.
FILE-CONTROL.
    SELECT REMOTE-LOG-SEGMENTS
        ASSIGN TO REMOTE-LOG-SEGMENT-FILE.

DATA DIVISION.
FILE SECTION.
FD REMOTE-LOG-SEGMENTS.
   01 REMOTE-LOG-SEGMENT-RECORD.
      05 SEGMENT-ID PIC X(36).
      05 SEGMENT-START-OFFSET PIC 9(18).
      05 SEGMENT-END-OFFSET PIC 9(18).
      05 SEGMENT-MAX-TIMESTAMP PIC 9(18).
      05 BROKER-ID PIC 9(9).
      05 COPY-TIME PIC 9(18).
      05 SEGMENT-SIZE PIC 9(18).
      05 SEGMENT-LEADER-EPOCHS.
         10 OCCURS 1 TO 10 TIMES DEPENDING ON EPOCH-COUNT.
            15 EPOCH PIC 9(9).
            15 EPOCH-START-OFFSET PIC 9(18).
      05 IS-TXN-IDX-EMPTY PIC X.

WORKING-STORAGE SECTION.
01 RLM-CONFIG.
   05 REMOTE-STORAGE-MANAGER-CLASS-PATH PIC X(200).
   05 REMOTE-STORAGE-MANAGER-CLASS-NAME PIC X(100).
   05 REMOTE-LOG-METADATA-MANAGER-CLASS-PATH PIC X(200).
   05 REMOTE-LOG-METADATA-MANAGER-CLASS-NAME PIC X(100).
   05 REMOTE-STORAGE-MANAGER-PROPS.
      10 OCCURS 1 TO 100 TIMES DEPENDING ON PROP-COUNT.
         15 PROP-NAME PIC X(100).
         15 PROP-VALUE PIC X(200).
   05 REMOTE-LOG-METADATA-MANAGER-PROPS.
      10 OCCURS 1 TO 100 TIMES DEPENDING ON PROP-COUNT.
         15 PROP-NAME PIC X(100).
         15 PROP-VALUE PIC X(200).
   05 REMOTE-LOG-MANAGER-COPY-MAX-BYTES-PER-SEC PIC 9(18).
   05 REMOTE-LOG-MANAGER-COPY-NUM-QUOTA-SAMPLES PIC 9(9).
   05 REMOTE-LOG-MANAGER-COPY-QUOTA-WINDOW-SIZE-SECS PIC 9(9).
   05 REMOTE-LOG-MANAGER-FETCH-MAX-BYTES-PER-SEC PIC 9(18).
   05 REMOTE-LOG-MANAGER-FETCH-NUM-QUOTA-SAMPLES PIC 9(9).
   05 REMOTE-LOG-MANAGER-FETCH-QUOTA-WINDOW-SIZE-SECS PIC 9(9).
   05 REMOTE-LOG-MANAGER-TASK-INTERVAL-MS PIC 9(18).
   05 REMOTE-LOG-MANAGER-COPIER-THREAD-POOL-SIZE PIC 9(9).
   05 REMOTE-LOG-MANAGER-EXPIRATION-THREAD-POOL-SIZE PIC 9(9).
   05 REMOTE-LOG-MANAGER-THREAD-POOL-SIZE PIC 9(9).
   05 REMOTE-LOG-INDEX-FILE-CACHE-TOTAL-SIZE-BYTES PIC 9(18).
   05 REMOTE-LOG-REAPER-THREAD-NAME-PATTERN PIC X(50).
   05 REMOTE-LOG-REAPER-THREAD-POOL-SIZE PIC 9(9).
   05 REMOTE-LOG-REAPER-MAX-PENDING-TASKS PIC 9(9).
   05 REMOTE-LOG-METADATA-CUSTOM-METADATA-MAX-BYTES PIC 9(18).

01 BROKER-ID PIC 9(9).
01 LOG-DIR PIC X(200).
01 CLUSTER-ID PIC X(36).
01 IS-REMOTE-LOG-MANAGER-CONFIGURED PIC X VALUE "N".

PROCEDURE DIVISION.
MAIN-PROCEDURE.
    PERFORM INITIALIZE-REMOTE-LOG-MANAGER.
    PERFORM ON-LEADERSHIP-CHANGE.
    PERFORM STOP-PARTITIONS.
    PERFORM CLOSE-REMOTE-LOG-MANAGER.
    STOP RUN.

INITIALIZE-REMOTE-LOG-MANAGER.
    PERFORM CREATE-REMOTE-STORAGE-MANAGER.
    PERFORM CREATE-REMOTE-LOG-METADATA-MANAGER.
    PERFORM CREATE-RLM-COPY-QUOTA-MANAGER.
    PERFORM CREATE-RLM-FETCH-QUOTA-MANAGER.
    PERFORM CREATE-REMOTE-INDEX-CACHE.
    PERFORM CREATE-RLM-COPY-THREAD-POOL.
    PERFORM CREATE-RLM-EXPIRATION-THREAD-POOL.
    PERFORM CREATE-RLM-FOLLOWER-THREAD-POOL.
    MOVE "Y" TO IS-REMOTE-LOG-MANAGER-CONFIGURED.

ON-LEADERSHIP-CHANGE.
    PERFORM FILTER-PARTITIONS.
    PERFORM CACHE-TOPIC-PARTITION-IDS.
    PERFORM HANDLE-LEADER-PARTITIONS.
    PERFORM HANDLE-FOLLOWER-PARTITIONS.

STOP-PARTITIONS.
    PERFORM STOP-LEADER-COPY-RLM-TASKS.
    PERFORM STOP-RLM-PARTITIONS.

CLOSE-REMOTE-LOG-MANAGER.
    PERFORM CANCEL-RLM-TASKS.
    PERFORM CLOSE-REMOTE-STORAGE-MANAGER.
    PERFORM CLOSE-REMOTE-LOG-METADATA-MANAGER.
    PERFORM CLOSE-REMOTE-INDEX-CACHE.
    PERFORM CLOSE-RLM-COPY-THREAD-POOL.
    PERFORM CLOSE-RLM-EXPIRATION-THREAD-POOL.
    PERFORM CLOSE-RLM-FOLLOWER-THREAD-POOL.
    PERFORM CLOSE-REMOTE-STORAGE-READER-THREAD-POOL.
    PERFORM REMOVE-METRICS.

CREATE-REMOTE-STORAGE-MANAGER.
    CREATE REMOTE-STORAGE-MANAGER USING REMOTE-STORAGE-MANAGER-CLASS-PATH, REMOTE-STORAGE-MANAGER-CLASS-NAME.
    PERFORM CONFIGURE-REMOTE-STORAGE-MANAGER.

CREATE-REMOTE-LOG-METADATA-MANAGER.
    CREATE REMOTE-LOG-METADATA-MANAGER USING REMOTE-LOG-METADATA-MANAGER-CLASS-PATH, REMOTE-LOG-METADATA-MANAGER-CLASS-NAME.
    PERFORM CONFIGURE-REMOTE-LOG-METADATA-MANAGER.

CREATE-RLM-COPY-QUOTA-MANAGER.
    CREATE RLM-COPY-QUOTA-MANAGER USING REMOTE-LOG-MANAGER-COPY-MAX-BYTES-PER-SEC, REMOTE-LOG-MANAGER-COPY-NUM-QUOTA-SAMPLES, REMOTE-LOG-MANAGER-COPY-QUOTA-WINDOW-SIZE-SECS.

CREATE-RLM-FETCH-QUOTA-MANAGER.
    CREATE RLM-FETCH-QUOTA-MANAGER USING REMOTE-LOG-MANAGER-FETCH-MAX-BYTES-PER-SEC, REMOTE-LOG-MANAGER-FETCH-NUM-QUOTA-SAMPLES, REMOTE-LOG-MANAGER-FETCH-QUOTA-WINDOW-SIZE-SECS.

CREATE-REMOTE-INDEX-CACHE.
    CREATE REMOTE-INDEX-CACHE USING REMOTE-LOG-INDEX-FILE-CACHE-TOTAL-SIZE-BYTES, REMOTE-STORAGE-MANAGER, LOG-DIR.

CREATE-RLM-COPY-THREAD-POOL.
    CREATE RLM-COPY-THREAD-POOL USING REMOTE-LOG-MANAGER-COPIER-THREAD-POOL-SIZE, "RLMCopyThreadPool", REMOTE-LOG-REAPER-THREAD-NAME-PATTERN.

CREATE-RLM-EXPIRATION-THREAD-POOL.
    CREATE RLM-EXPIRATION-THREAD-POOL USING REMOTE-LOG-MANAGER-EXPIRATION-THREAD-POOL-SIZE, "RLMExpirationThreadPool", REMOTE-LOG-REAPER-THREAD-NAME-PATTERN.

CREATE-RLM-FOLLOWER-THREAD-POOL.
    CREATE RLM-FOLLOWER-THREAD-POOL USING REMOTE-LOG-MANAGER-THREAD-POOL-SIZE, "RLMFollowerScheduledThreadPool", REMOTE-LOG-REAPER-THREAD-NAME-PATTERN.

CONFIGURE-REMOTE-STORAGE-MANAGER.
    SET BROKER-ID-CONFIG OF REMOTE-STORAGE-MANAGER TO BROKER-ID.
    PERFORM VARYING I FROM 1 BY 1 UNTIL I > PROP-COUNT
        SET PROP-NAME OF REMOTE-STORAGE-MANAGER-PROPS(I) TO PROP-NAME(I)
        SET PROP-VALUE OF REMOTE-STORAGE-MANAGER-PROPS(I) TO PROP-VALUE(I)
    END-PERFORM.
    CONFIGURE REMOTE-STORAGE-MANAGER USING REMOTE-STORAGE-MANAGER-PROPS.

CONFIGURE-REMOTE-LOG-METADATA-MANAGER.
    SET BOOTSTRAP-SERVERS OF REMOTE-LOG-METADATA-MANAGER-PROPS TO ENDPOINT-HOST ":" ENDPOINT-PORT.
    SET SECURITY-PROTOCOL OF REMOTE-LOG-METADATA-MANAGER-PROPS TO ENDPOINT-SECURITY-PROTOCOL.
    PERFORM VARYING I FROM 1 BY 1 UNTIL I > PROP-COUNT
        SET PROP-NAME OF REMOTE-LOG-METADATA-MANAGER-PROPS(I) TO PROP-NAME(I)
        SET PROP-VALUE OF REMOTE-LOG-METADATA-MANAGER-PROPS(I) TO PROP-VALUE(I)
    END-PERFORM.
    SET BROKER-ID-CONFIG OF REMOTE-LOG-METADATA-MANAGER-PROPS TO BROKER-ID.
    SET LOG-DIR-CONFIG OF REMOTE-LOG-METADATA-MANAGER-PROPS TO LOG-DIR.
    SET CLUSTER-ID OF REMOTE-LOG-METADATA-MANAGER-PROPS TO CLUSTER-ID.
    CONFIGURE REMOTE-LOG-METADATA-MANAGER USING REMOTE-LOG-METADATA-MANAGER-PROPS.

FILTER-PARTITIONS.
    MOVE PARTITIONS-BECOME-LEADER TO WS-LEADER-PARTITIONS.
    MOVE PARTITIONS-BECOME-FOLLOWER TO WS-FOLLOWER-PARTITIONS.
    FILTER WS-LEADER-PARTITIONS KEEPING ONLY PARTITIONS WHERE REMOTE-LOG-ENABLED = "Y".
    FILTER WS-FOLLOWER-PARTITIONS KEEPING ONLY PARTITIONS WHERE REMOTE-LOG-ENABLED = "Y".

CACHE-TOPIC-PARTITION-IDS.
    PERFORM VARYING PARTITION IN WS-LEADER-PARTITIONS
        MOVE TOPIC-ID(PARTITION) TO TOPIC-ID-BY-PARTITION(PARTITION)
    END-PERFORM.
    PERFORM VARYING PARTITION IN WS-FOLLOWER-PARTITIONS 
        MOVE TOPIC-ID(PARTITION) TO TOPIC-ID-BY-PARTITION(PARTITION)
    END-PERFORM.
    CALL REMOTE-LOG-METADATA-MANAGER TO ON-PARTITION-LEADERSHIP-CHANGES USING WS-LEADER-PARTITIONS, WS-FOLLOWER-PARTITIONS.

HANDLE-LEADER-PARTITIONS.
    PERFORM VARYING PARTITION IN WS-LEADER-PARTITIONS
        IF LEADER-COPY-RLM-TASKS(PARTITION) IS NULL
            PERFORM START-LEADER-COPY-TASK USING PARTITION
        ELSE
            PERFORM CONVERT-LEADER-COPY-TASK USING PARTITION
        END-IF
        IF LEADER-EXPIRATION-RLM-TASKS(PARTITION) IS NULL
            PERFORM START-LEADER-EXPIRATION-TASK USING PARTITION
        ELSE
            PERFORM CONVERT-LEADER-EXPIRATION-TASK USING PARTITION
        END-IF
    END-PERFORM.

HANDLE-FOLLOWER-PARTITIONS. 
    PERFORM VARYING PARTITION IN WS-FOLLOWER-PARTITIONS
        IF FOLLOWER-RLM-TASKS(PARTITION) IS NULL
            PERFORM START-FOLLOWER-TASK USING PARTITION
        ELSE
            PERFORM CONVERT-FOLLOWER-TASK USING PARTITION
        END-IF
    END-PERFORM.

STOP-LEADER-COPY-RLM-TASKS.
    PERFORM VARYING PARTITION IN PARTITIONS
        IF TOPIC-ID-BY-PARTITION(PARTITION) IS NOT NULL
            CALL LEADER-COPY-RLM-TASKS(PARTITION) TO CANCEL
        END-IF
    END-PERFORM.

STOP-RLM-PARTITIONS.
    PERFORM VARYING STOP-PARTITION IN STOP-PARTITIONS
        CALL REMOTE-LOG-METADATA-MANAGER TO ON-STOP-PARTITIONS USING STOP-PARTITION
        IF DELETE-REMOTE-LOG OF STOP-PARTITION = "Y"
            PERFORM DELETE-REMOTE-LOG-PARTITION USING TOPIC-ID-BY-PARTITION(STOP-PARTITION.TOPIC-PARTITION), STOP-PARTITION.TOPIC-PARTITION
        END-IF
    END-PERFORM.
    CALL REMOTE-LOG-METADATA-MANAGER TO CLEAR-TOPIC-ID-BY-PARTITION-MAP USING STOP-PARTITIONS.

START-LEADER-COPY-TASK.
    CREATE RLM-COPY-TASK USING TOPIC-ID-BY-PARTITION(PARTITION), REMOTE-LOG-METADATA-CUSTOM-METADATA-MAX-BYTES.
    SCHEDULE RLM-COPY-TASK IN RLM-COPY-THREAD-POOL EVERY REMOTE-LOG-MANAGER-TASK-INTERVAL-MS MILLISECONDS.
    MOVE RLM-COPY-TASK TO LEADER-COPY-RLM-TASKS(PARTITION).

START-LEADER-EXPIRATION-TASK.
    CREATE RLM-EXPIRATION-TASK USING TOPIC-ID-BY-PARTITION(PARTITION).
    SCHEDULE RLM-EXPIRATION-TASK IN RLM-EXPIRATION-THREAD-POOL EVERY REMOTE-LOG-MANAGER-TASK-INTERVAL-MS MILLISECONDS.
    MOVE RLM-EXPIRATION-TASK TO LEADER-EXPIRATION-RLM-TASKS(PARTITION).

START-FOLLOWER-TASK.
    CREATE RLM-FOLLOWER-TASK USING TOPIC-ID-BY-PARTITION(PARTITION).
    SCHEDULE RLM-FOLLOWER-TASK IN RLM-FOLLOWER-THREAD-POOL EVERY REMOTE-LOG-MANAGER-TASK-INTERVAL-MS MILLISECONDS.
    MOVE RLM-FOLLOWER-TASK TO FOLLOWER-RLM-TASKS(PARTITION).

CONVERT-LEADER-COPY-TASK.
    CALL LEADER-COPY-RLM-TASKS(PARTITION) TO CONVERT-TO-LEADER-TASK.

CONVERT-LEADER-EXPIRATION-TASK.
    CALL LEADER-EXPIRATION-RLM-TASKS(PARTITION) TO CONVERT-TO-LEADER-TASK.

CONVERT-FOLLOWER-TASK.
    CALL FOLLOWER-RLM-TASKS(PARTITION) TO CONVERT-TO-FOLLOWER-TASK.

DELETE-REMOTE-LOG-PARTITION.
    PERFORM VARYING SEGMENT-METADATA IN REMOTE-LOG-SEGMENTS-FOR-PARTITION(TOPIC-ID, PARTITION)
        CALL REMOTE-LOG-METADATA-MANAGER TO UPDATE-REMOTE-LOG-SEGMENT-METADATA USING SEGMENT-ID(SEGMENT-METADATA), TIME-MILLISECONDS(), SEGMENT-METADATA(CUSTOM-METADATA), "DELETE_SEGMENT_STARTED", BROKER-ID
        CALL REMOTE