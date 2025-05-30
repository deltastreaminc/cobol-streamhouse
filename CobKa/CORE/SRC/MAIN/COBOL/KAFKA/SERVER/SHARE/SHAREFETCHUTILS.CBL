IDENTIFICATION DIVISION.
PROGRAM-ID. SHARE-FETCH-UTILS.

ENVIRONMENT DIVISION.
CONFIGURATION SECTION.
REPOSITORY.
    FUNCTION LOG-FACTORY
        RETURNING LOGGER.

DATA DIVISION.
WORKING-STORAGE SECTION.
01 RESPONSE-MAP.
   05 TOPIC-ID-PARTITION-ITEM OCCURS 1 TO 32767 TIMES
      DEPENDING ON RESPONSE-MAP-SIZE.
      10 TOPIC-ID-PARTITION PIC X(256).
      10 PARTITION-DATA.
         15 PARTITION-INDEX PIC 9(9) BINARY.
         15 RECORDS-FIELD PIC X(32767).
         15 ERROR-CODE PIC 9(9) BINARY.
         15 ERROR-MESSAGE PIC X(256).
         15 ACQUIRED-RECORDS-FIELD PIC X(32767).
   05 RESPONSE-MAP-SIZE PIC 9(9) BINARY.

01 ACQUIRED-RECORDS-COUNT PIC 9(9) BINARY.

01 LOGGER PIC X(32767).

PROCEDURE DIVISION.

PROCESS-FETCH-RESPONSE.
    MOVE 0 TO RESPONSE-MAP-SIZE.
    MOVE 0 TO ACQUIRED-RECORDS-COUNT.
    PERFORM VARYING TOPIC-ID-PARTITION-ITEM FROM 1 BY 1
        UNTIL TOPIC-ID-PARTITION-ITEM > SIZE OF RESPONSE-DATA
        MOVE ENTRY TOPIC-ID-PARTITION-ITEM OF RESPONSE-DATA
            TO TOPIC-ID-PARTITION OF TOPIC-ID-PARTITION-ITEM
        MOVE PARTITION-INDEX OF ENTRY TOPIC-ID-PARTITION-ITEM OF RESPONSE-DATA
            TO PARTITION-INDEX OF TOPIC-ID-PARTITION-ITEM
        IF FETCH-PARTITION-DATA-ERROR-CODE OF ENTRY TOPIC-ID-PARTITION-ITEM OF RESPONSE-DATA
            NOT = ZERO
            MOVE NULL TO RECORDS-FIELD OF TOPIC-ID-PARTITION-ITEM
            MOVE FETCH-PARTITION-DATA-ERROR-CODE OF ENTRY TOPIC-ID-PARTITION-ITEM OF RESPONSE-DATA
                TO ERROR-CODE OF TOPIC-ID-PARTITION-ITEM
            MOVE FETCH-PARTITION-DATA-ERROR-MESSAGE OF ENTRY TOPIC-ID-PARTITION-ITEM OF RESPONSE-DATA
                TO ERROR-MESSAGE OF TOPIC-ID-PARTITION-ITEM
            MOVE NULL TO ACQUIRED-RECORDS-FIELD OF TOPIC-ID-PARTITION-ITEM
        ELSE
            PERFORM ACQUIRE-RECORDS
            MOVE ACQUIRED-RECORDS TO ACQUIRED-RECORDS-FIELD OF TOPIC-ID-PARTITION-ITEM
            ADD ACQUIRED-RECORDS-COUNT TO ACQUIRED-RECORDS-COUNT
            MOVE FETCH-PARTITION-DATA-RECORDS OF ENTRY TOPIC-ID-PARTITION-ITEM OF RESPONSE-DATA
                TO RECORDS-FIELD OF TOPIC-ID-PARTITION-ITEM
        END-IF
        ADD 1 TO RESPONSE-MAP-SIZE
    END-PERFORM.
    MOVE RESPONSE-MAP TO RESULT.

ACQUIRE-RECORDS.
    CALL "SHARE-PARTITION" USING TOPIC-ID-PARTITION, MEMBER-ID, BATCH-SIZE,
        MAX-FETCH-RECORDS - ACQUIRED-RECORDS-COUNT, FETCH-PARTITION-DATA
        RETURNING ACQUIRED-RECORDS, ACQUIRED-RECORDS-COUNT.
    LOG-TRACE "Acquired records: {} for topicIdPartition: {}" USING ACQUIRED-RECORDS, TOPIC-ID-PARTITION.

OFFSET-FOR-EARLIEST-TIMESTAMP.
    CALL "REPLICA-MANAGER" USING TOPIC-PARTITION, LISTOFFSETS-EARLIEST-TIMESTAMP, ISOLATION-LEVEL-EMPTY,
        LEADER-EPOCH, TRUE RETURNING TIMESTAMP-AND-OFFSET.
    IF TIMESTAMP-AND-OFFSET IS EMPTY
        RAISE OFFSETNOTAVAILABLEEXCEPTION "Offset for earliest timestamp not found for topic partition: " TOPIC-PARTITION
    END-IF.
    MOVE TIMESTAMP-AND-OFFSET-OFFSET TO RESULT.

OFFSET-FOR-LATEST-TIMESTAMP.
    CALL "REPLICA-MANAGER" USING TOPIC-PARTITION, LISTOFFSETS-LATEST-TIMESTAMP, ISOLATION-LEVEL-READ-UNCOMMITTED,
        LEADER-EPOCH, TRUE RETURNING TIMESTAMP-AND-OFFSET.
    IF TIMESTAMP-AND-OFFSET IS EMPTY
        RAISE OFFSETNOTAVAILABLEEXCEPTION "Offset for latest timestamp not found for topic partition: " TOPIC-PARTITION
    END-IF.
    MOVE TIMESTAMP-AND-OFFSET-OFFSET TO RESULT.

OFFSET-FOR-TIMESTAMP.
    CALL "REPLICA-MANAGER" USING TOPIC-PARTITION, TIMESTAMP-TO-SEARCH, ISOLATION-LEVEL-READ-UNCOMMITTED,
        LEADER-EPOCH, TRUE RETURNING TIMESTAMP-AND-OFFSET.
    IF TIMESTAMP-AND-OFFSET IS EMPTY
        RAISE OFFSETNOTAVAILABLEEXCEPTION "Offset for timestamp " TIMESTAMP-TO-SEARCH " not found for topic partition: " TOPIC-PARTITION
    END-IF.
    MOVE TIMESTAMP-AND-OFFSET-OFFSET TO RESULT.

LEADER-EPOCH.
    CALL "REPLICA-MANAGER" USING TOPIC-PARTITION RETURNING PARTITION.
    IF NOT PARTITION-IS-LEADER
        LOG-DEBUG "The broker is not the leader for topic partition: {}-{}" USING TOPIC-PARTITION-TOPIC, TOPIC-PARTITION-PARTITION
        RAISE NOTLEADERORFOLLOWEREXCEPTION
    END-IF.
    MOVE PARTITION-LEADER-EPOCH TO RESULT.

PARTITION.
    CALL "REPLICA-MANAGER" USING TOPIC-PARTITION RETURNING PARTITION.
    IF NOT PARTITION-IS-LEADER
        LOG-DEBUG "The broker is not the leader for topic partition: {}-{}" USING TOPIC-PARTITION-TOPIC, TOPIC-PARTITION-PARTITION
        RAISE NOTLEADERORFOLLOWEREXCEPTION
    END-IF.
    MOVE PARTITION TO RESULT.

END PROGRAM SHARE-FETCH-UTILS.