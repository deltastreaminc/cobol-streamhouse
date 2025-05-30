IDENTIFICATION DIVISION.
PROGRAM-ID. KafkaRequestHandler.

ENVIRONMENT DIVISION.
CONFIGURATION SECTION.
SPECIAL-NAMES.
    CURRENT-DATE IS TIME.

DATA DIVISION.
WORKING-STORAGE SECTION.
01 SHUTDOWN-COMPLETE PIC 9(9) COMP VALUE 1.
01 STOPPED PIC X(1) VALUE 'N'.
01 REQUEST-LOCAL PIC X(256).
01 REQUEST-CHANNEL PIC X(256).
01 CURRENT-REQUEST PIC X(256).
01 AGGREGATE-IDLE-METER PIC 9(18) COMP-3.
01 TOTAL-HANDLER-THREADS PIC 9(9) COMP.
01 BROKER-ID PIC 9(9) COMP.
01 NODE-NAME PIC X(12) VALUE 'broker'.

LINKAGE SECTION.
01 REQUEST PIC X(256).
01 CALLBACK-REQUEST PIC X(256).
01 ASYNC-COMPLETION-CALLBACK PIC X(256).
01 CALLBACK-FUN PIC X(256).

PROCEDURE DIVISION.

main-procedure.
    PERFORM INIT-HANDLER.
    PERFORM HANDLE-REQUESTS UNTIL STOPPED.
    PERFORM COMPLETE-SHUTDOWN.
    STOP RUN.

INIT-HANDLER.
    SET REQUEST-CHANNEL TO REQUEST-CHANNEL.
    SET CURRENT-REQUEST TO REQUEST.

HANDLE-REQUESTS.
    PERFORM UPDATE-IDLE-METER.
    PERFORM RECEIVE-REQUEST.
    EVALUATE REQUEST
        WHEN SHUTDOWN-REQUEST
            PERFORM COMPLETE-SHUTDOWN
            EXIT PROGRAM
        WHEN CALLBACK-REQUEST
            PERFORM HANDLE-CALLBACK-REQUEST
        WHEN OTHER-REQUEST
            PERFORM HANDLE-REQUEST
        WHEN WAKEUP-REQUEST
            CONTINUE
        WHEN NULL
            CONTINUE.

UPDATE-IDLE-METER.
    MOVE TIME TO START-SELECT-TIME.
    COMPUTE IDLE-TIME = TIME - START-SELECT-TIME.
    DIVIDE IDLE-TIME BY TOTAL-HANDLER-THREADS GIVING DISCOUNTED-IDLE-TIME.
    ADD DISCOUNTED-IDLE-TIME TO AGGREGATE-IDLE-METER.

RECEIVE-REQUEST.
    CALL "requestChannel.receiveRequest" USING 300 RETURNING REQUEST.

HANDLE-CALLBACK-REQUEST.
    MOVE CALLBACK-REQUEST TO ORIGINAL-REQUEST.
    PERFORM RESET-CALLBACK-TIMES.
    SET CURRENT-REQUEST TO ORIGINAL-REQUEST.
    CALL CALLBACK-FUN USING REQUEST-LOCAL.
    PERFORM TRY-COMPLETE-ACTIONS.
    SET CALLBACK-REQUEST-COMPLETE-TIME TO TIME.
    SET CURRENT-REQUEST TO NULL.

HANDLE-REQUEST.
    MOVE TIME TO REQUEST-DEQUEUE-TIME.
    SET CURRENT-REQUEST TO REQUEST.
    CALL "apis.handle" USING REQUEST, REQUEST-LOCAL.
    PERFORM TRY-COMPLETE-ACTIONS.
    SET CURRENT-REQUEST TO NULL.
    CALL "request.releaseBuffer".

TRY-COMPLETE-ACTIONS.
    CALL "apis.tryCompleteActions".

RESET-CALLBACK-TIMES.
    IF ORIGINAL-REQUEST-CALLBACK-DEQUEUE-TIME IS DEFINED
        COMPUTE PREV-CALLBACKS-TIME = ORIGINAL-REQUEST-CALLBACK-COMPLETE-TIME - ORIGINAL-REQUEST-CALLBACK-DEQUEUE-TIME
        SET ORIGINAL-REQUEST-CALLBACK-COMPLETE-TIME TO NULL
        SET ORIGINAL-REQUEST-CALLBACK-DEQUEUE-TIME TO TIME - PREV-CALLBACKS-TIME
    ELSE
        SET ORIGINAL-REQUEST-CALLBACK-DEQUEUE-TIME TO TIME.

COMPLETE-SHUTDOWN.
    CALL "requestLocal.close".
    SET REQUEST-CHANNEL TO NULL.
    SET SHUTDOWN-COMPLETE TO 0.

STOP-HANDLER.
    SET STOPPED TO 'Y'.

SHUTDOWN-HANDLER.
    CALL "requestChannel.sendShutdownRequest".

AWAIT-SHUTDOWN.
    CALL "shutdownComplete.await".