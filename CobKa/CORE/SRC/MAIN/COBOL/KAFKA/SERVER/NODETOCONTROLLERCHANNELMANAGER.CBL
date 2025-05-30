IDENTIFICATION DIVISION.
PROGRAM-ID. NODE-TO-CONTROLLER-CHANNEL-MANAGER.

ENVIRONMENT DIVISION.
CONFIGURATION SECTION.
REPOSITORY.
    COPY "KAFKA-COMMON-ENTITIES".

DATA DIVISION.
WORKING-STORAGE SECTION.
01 WS-CONTROLLER-INFORMATION.
   05 WS-CONTROLLER-NODE             PIC X(32).
   05 WS-CONTROLLER-LISTENER-NAME    PIC X(32).
   05 WS-CONTROLLER-SECURITY-PROTOCOL PIC X(32).
   05 WS-CONTROLLER-SASL-MECHANISM    PIC X(32).
01 WS-MANUAL-METADATA-UPDATER        OBJECT REFERENCE.
01 WS-API-VERSIONS                   OBJECT REFERENCE.
01 WS-REQUEST-THREAD                 OBJECT REFERENCE.
01 WS-ACTIVE-CONTROLLER              OBJECT REFERENCE.
01 WS-REQUEST-QUEUE                  OBJECT REFERENCE.
01 WS-STARTED                        PIC X(1) VALUE 'N'.

PROCEDURE DIVISION.

START-UP.
    MOVE 'Y' TO WS-STARTED.
    CALL "NEW" USING RFA-CONTROLLER-NODE-PROVIDER
                  WS-CONTROLLER-INFORMATION
                  WS-MANUAL-METADATA-UPDATER
                  WS-API-VERSIONS
    RETURNING WS-REQUEST-THREAD.
    CALL "START" USING WS-REQUEST-THREAD.

SHUT-DOWN.
    CALL "SHUTDOWN" USING WS-REQUEST-THREAD.
    DISPLAY "Node to controller channel manager shutdown".

SEND-REQUEST.
    MOVE FUNCTION CURRENT-DATE TO WS-REQUEST-QUEUE.
    MOVE REQUEST-BUILDER TO WS-REQUEST-QUEUE.
    MOVE CALLBACK TO WS-REQUEST-QUEUE.
    CALL "ENQUEUE" USING WS-REQUEST-QUEUE.
    IF WS-ACTIVE-CONTROLLER NOT NULL
        CALL "WAKEUP" USING WS-REQUEST-THREAD.

CONTROLLER-API-VERSIONS.
    MOVE WS-ACTIVE-CONTROLLER TO RETURN-VALUE.

GET-TIMEOUT-MS.
    MOVE WS-RETRYMAX-TIMEOUT-MS TO RETURN-VALUE.

PROCEDURE DIVISION USING REQUEST-BUILDER, CALLBACK.

STOP RUN.