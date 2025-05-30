IDENTIFICATION DIVISION.
PROGRAM-ID. KAFKA-METRICS-REPORTER.

ENVIRONMENT DIVISION.
CONFIGURATION SECTION.
INPUT-OUTPUT SECTION.
FILE-CONTROL.

DATA DIVISION.
WORKING-STORAGE SECTION.
01 REPORTER-STARTED PIC X(1) VALUE 'N'.
01 REPORTERS PIC X(256) VALUE SPACES.

PROCEDURE DIVISION.
START-REPORTERS.
    MOVE 'Y' TO REPORTER-STARTED.
    INITIALIZE REPORTERS.
    PERFORM VARYING IDX FROM 1 BY 1 UNTIL IDX > FUNCTION LENGTH(METRICSCONFIG-REPORTERS)
        MOVE METRICSCONFIG-REPORTERS(IDX) TO REPORTER-TYPE
        INVOKE CORE-UTILS "createObject" USING REPORTER-TYPE RETURNING REPORTER
        CALL REPORTER "init" USING VERIFIABLE-PROPERTIES
        ADD REPORTER TO REPORTERS
        IF REPORTER IS INSTANCE OF KAFKA-METRICS-REPORTER-MBEAN
            CALL CORE-UTILS "registerMBean" USING REPORTER REPORTER-MBEAN-NAME
        END-IF
    END-PERFORM.

START-REPORTER.
    CALL REPORTER "startReporter" USING POLLING-PERIOD.

STOP-REPORTER.
    CALL REPORTER "stopReporter".

IDENTIFICATION DIVISION.
PROGRAM-ID. KAFKA-METRICS-CONFIG.

DATA DIVISION.
WORKING-STORAGE SECTION.
01 REPORTERS PIC X(256) VALUE SPACES.

PROCEDURE DIVISION.
INIT.
    MOVE VERIFIABLE-PROPERTIES-REPORTERS TO REPORTERS.

IDENTIFICATION DIVISION.
PROGRAM-ID. CORE-UTILS.

PROCEDURE DIVISION.
CREATEOBJECT.
    *> Implementation to create an object of the given class

REGISTERMBEAN.
    *> Implementation to register the MBean with the given name