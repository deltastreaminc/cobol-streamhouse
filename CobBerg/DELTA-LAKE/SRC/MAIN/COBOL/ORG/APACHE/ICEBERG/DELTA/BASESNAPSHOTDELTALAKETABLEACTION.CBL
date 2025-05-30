IDENTIFICATION DIVISION.
PROGRAM-ID. BASESNAPSHOTDELTALAKETA.

ENVIRONMENT DIVISION.
INPUT-OUTPUT SECTION.
FILE-CONTROL.
    SELECT DELTA-LOG-FILE
        ASSIGN TO DATASET-XT
        ORGANIZATION IS SEQUENTIAL.
    SELECT ICEBERG-TABLE-FILE
        ASSIGN TO DATASET-YT
        ORGANIZATION IS SEQUENTIAL.

DATA DIVISION.
FILE SECTION.
FD DELTA-LOG-FILE.
01 DELTA-LOG-RECORD.
    05 DELTA-VERSION               PIC 9(18).
    05 DELTA-TIMESTAMP             PIC 9(18).
    05 DELTA-ACTION                PIC X.
        88 DELTA-ACTION-ADDFILE    VALUE 'A'.
        88 DELTA-ACTION-REMOVEFILE VALUE 'R'.
    05 DELTA-PATH                  PIC X(1024).
    05 DELTA-PARTVALUES            PIC X(1024).
    05 DELTA-FILESIZE              PIC 9(18).

FD ICEBERG-TABLE-FILE.
01 ICEBERG-TABLE-RECORD.
    05 ICEBERG-PATH                PIC X(1024).
    05 ICEBERG-FORMAT              PIC X(10).
    05 ICEBERG-FILESIZE            PIC 9(18).
    05 ICEBERG-METRICS             PIC X(1024).
    05 ICEBERG-PARTVALUES          PIC X(1024).

WORKING-STORAGE SECTION.
01 WS-PROPERTIES.
    05 WS-PROP-SNAPSHOT-SOURCE     PIC X(10)   VALUE 'delta'.
    05 WS-PROP-ORIGINAL-LOCATION   PIC X(1024).
    05 WS-PROP-ADDITIONAL          PIC X(1024).

01 WS-DELTA-LOG-INFO.
    05 WS-DELTA-LOG-PATH           PIC X(1024).
    05 WS-DELTA-START-VERSION      PIC 9(18).
    05 WS-DELTA-LATEST-VERSION     PIC 9(18).

01 WS-ICEBERG-INFO.
    05 WS-ICEBERG-CATALOG          PIC X(50).
    05 WS-ICEBERG-IDENTIFIER       PIC X(50).
    05 WS-ICEBERG-LOCATION         PIC X(1024).

01 WS-FILE-DETAILS.
    05 WS-FILE-PATH                PIC X(1024).
    05 WS-FILE-FORMAT              PIC X(10).
    05 WS-FILE-SIZE                PIC 9(18).
    05 WS-FILE-METRICS             PIC X(1024).
    05 WS-FILE-PARTVALUES          PIC X(1024).

PROCEDURE DIVISION.
MAIN-PROCEDURE.
    PERFORM INITIALIZE-DELTA-LOG.
    PERFORM INITIALIZE-ICEBERG-INFO.
    PERFORM MIGRATE-DELTA-TO-ICEBERG.
    STOP RUN.

INITIALIZE-DELTA-LOG.
    OPEN INPUT DELTA-LOG-FILE.
    READ DELTA-LOG-FILE INTO WS-DELTA-LOG-INFO
        END-READ.
    CLOSE DELTA-LOG-FILE.

INITIALIZE-ICEBERG-INFO.
    MOVE WS-DELTA-LOG-PATH TO WS-ICEBERG-LOCATION.
    MOVE 'MY-ICEBERG-CATALOG' TO WS-ICEBERG-CATALOG.
    MOVE 'MY-ICEBERG-TABLE' TO WS-ICEBERG-IDENTIFIER.

MIGRATE-DELTA-TO-ICEBERG.
    OPEN OUTPUT ICEBERG-TABLE-FILE.
    PERFORM UNTIL WS-DELTA-START-VERSION > WS-DELTA-LATEST-VERSION
        PERFORM COMMIT-DELTA-SNAPSHOT
        ADD 1 TO WS-DELTA-START-VERSION
    END-PERFORM.
    CLOSE ICEBERG-TABLE-FILE.

COMMIT-DELTA-SNAPSHOT.
    MOVE WS-DELTA-START-VERSION TO DELTA-VERSION.
    READ DELTA-LOG-FILE INTO DELTA-LOG-RECORD
        AT END
            MOVE WS-DELTA-LATEST-VERSION TO DELTA-VERSION
    END-READ.

    IF DELTA-ACTION-ADDFILE
        PERFORM PROCESS-ADDFILE
    ELSE IF DELTA-ACTION-REMOVEFILE
        PERFORM PROCESS-REMOVEFILE
    END-IF.

    PERFORM TAG-ICEBERG-SNAPSHOT.

PROCESS-ADDFILE.
    MOVE DELTA-PATH TO WS-FILE-PATH.
    MOVE DELTA-FILESIZE TO WS-FILE-SIZE.
    MOVE DELTA-PARTVALUES TO WS-FILE-PARTVALUES.
    PERFORM DETERMINE-FILE-FORMAT.
    PERFORM GET-FILE-METRICS.
    WRITE ICEBERG-TABLE-RECORD FROM WS-FILE-DETAILS.

PROCESS-REMOVEFILE.
    MOVE DELTA-PATH TO WS-FILE-PATH.
    MOVE DELTA-FILESIZE TO WS-FILE-SIZE.
    MOVE DELTA-PARTVALUES TO WS-FILE-PARTVALUES.
    PERFORM DETERMINE-FILE-FORMAT.
    PERFORM GET-FILE-METRICS.
    WRITE ICEBERG-TABLE-RECORD FROM WS-FILE-DETAILS.

DETERMINE-FILE-FORMAT.
    IF WS-FILE-PATH ENDS WITH '.parquet'
        MOVE 'PARQUET' TO WS-FILE-FORMAT
    ELSE
        MOVE 'UNKNOWN' TO WS-FILE-FORMAT
    END-IF.

GET-FILE-METRICS.
    MOVE 'SAMPLE-METRICS' TO WS-FILE-METRICS.

TAG-ICEBERG-SNAPSHOT.
    MOVE WS-DELTA-START-VERSION TO WS-PROP-SNAPSHOT-SOURCE.
    MOVE WS-DELTA-LOG-PATH TO WS-PROP-ORIGINAL-LOCATION.
    STRING 'delta-version-' DELTA-VERSION INTO WS-PROP-ADDITIONAL.
    STRING 'delta-ts-' DELTA-TIMESTAMP INTO WS-PROP-ADDITIONAL.
    WRITE ICEBERG-TABLE-RECORD FROM WS-PROPERTIES.