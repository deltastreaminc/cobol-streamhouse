IDENTIFICATION DIVISION.
PROGRAM-ID. BASEWRITEREMANIFESTS.

ENVIRONMENT DIVISION.
INPUT-OUTPUT SECTION.
FILE-CONTROL.
    SELECT MANIFESTFILE-INPUT
        ASSIGN TO ORGANIZATION SEQUENTIAL
        ACCESS MODE IS SEQUENTIAL.
    SELECT MANIFESTFILE-OUTPUT
        ASSIGN TO ORGANIZATION SEQUENTIAL
        ACCESS MODE IS SEQUENTIAL.

DATA DIVISION.
FILE SECTION.
FD MANIFESTFILE-INPUT.
01 MANIFESTFILE-INPUT-REC.
   05 MANIFEST-PATH PIC X(256).
   05 MANIFEST-PARTITIONSPECID PIC 9(9) BINARY.
   05 MANIFEST-CONTENT PIC X.
   05 MANIFEST-SNAPSHOTID PIC 9(18) BINARY.
   05 MANIFEST-SEQUENCENUMBER PIC 9(18) BINARY.
   05 MANIFEST-ADDEDFILESCOUNT PIC 9(9) BINARY.
   05 MANIFEST-EXISTINGFILESCOUNT PIC 9(9) BINARY.

FD MANIFESTFILE-OUTPUT.
01 MANIFESTFILE-OUTPUT-REC.
   05 MANIFEST-PATH PIC X(256).
   05 MANIFEST-PARTITIONSPECID PIC 9(9) BINARY.
   05 MANIFEST-CONTENT PIC X.
   05 MANIFEST-SNAPSHOTID PIC 9(18) BINARY.
   05 MANIFEST-SEQUENCENUMBER PIC 9(18) BINARY.
   05 MANIFEST-ADDEDFILESCOUNT PIC 9(9) BINARY.
   05 MANIFEST-EXISTINGFILESCOUNT PIC 9(9) BINARY.

WORKING-STORAGE SECTION.
01 MANIFEST-TARGET-SIZE-BYTES PIC 9(18) BINARY VALUE 0.
01 DELETED-MANIFESTS PIC 9(9) BINARY VALUE 0.
01 ADDED-MANIFESTS PIC 9(9) BINARY VALUE 0.
01 REWRITTEN-ADDED-MANIFESTS PIC 9(9) BINARY VALUE 0.
01 KEPT-MANIFESTS PIC 9(9) BINARY VALUE 0.
01 NEW-MANIFESTS PIC 9(9) BINARY VALUE 0.
01 REWRITTEN-MANIFESTS PIC 9(9) BINARY VALUE 0.
01 ENTRY-COUNT PIC 9(18) BINARY VALUE 0.
01 PARTITIONSPEC-TABLE.
   05 PARTITIONSPEC-ID PIC 9(9) BINARY OCCURS 9999 TIMES.
   05 PARTITIONSPEC-OBJECT OCCURS 9999 TIMES.
      10 PARTITIONSPEC-FIELDS PIC X(256) OCCURS 10 TIMES.

PROCEDURE DIVISION.

MAIN-PROCEDURE.
    PERFORM VALIDATE-DELETED-MANIFESTS.
    PERFORM PERFORM-REWRITE.
    PERFORM VALIDATE-FILES-COUNTS.
    PERFORM CLEAN-UNCOMMITTED.
    MOVE KEPT-MANIFESTS TO MANIFEST-KEPT-COUNT.
    MOVE NEW-MANIFESTS TO MANIFEST-CREATED-COUNT.
    MOVE REWRITTEN-MANIFESTS + DELETED-MANIFESTS TO MANIFEST-REPLACED-COUNT.
    MOVE ENTRY-COUNT TO ENTRIES-PROCESSED-COUNT.
    STOP RUN.

VALIDATE-DELETED-MANIFESTS.
    READ MANIFESTFILE-INPUT
        INTO MANIFESTFILE-INPUT-REC
        AT END GO TO VALIDATE-DELETED-MANIFESTS-EXIT.
    IF MANIFEST-CONTENT = 'D'
        AND MANIFEST-PATH NOT IN DELETED-MANIFESTS
        THEN
            DISPLAY 'Deleted manifest ' MANIFEST-PATH ' could not be found in the latest snapshot ' MANIFEST-SNAPSHOTID
            STOP RUN.
    GO TO VALIDATE-DELETED-MANIFESTS.
VALIDATE-DELETED-MANIFESTS-EXIT.
    EXIT.

PERFORM-REWRITE.
    PERFORM RESET.
    READ MANIFESTFILE-INPUT
        INTO MANIFESTFILE-INPUT-REC
        AT END GO TO PERFORM-REWRITE-EXIT.
    IF MANIFEST-CONTENT = 'D'
        OR NOT MATCH-PREDICATE(MANIFEST-PATH, MANIFEST-PARTITIONSPECID)
        THEN
            ADD 1 TO KEPT-MANIFESTS
            WRITE MANIFESTFILE-OUTPUT-REC FROM MANIFESTFILE-INPUT-REC
    ELSE
        ADD 1 TO REWRITTEN-MANIFESTS
        PERFORM PROCESS-MANIFEST.
    GO TO PERFORM-REWRITE.
PERFORM-REWRITE-EXIT.
    PERFORM CLOSE-WRITERS.
    EXIT.

PROCESS-MANIFEST.
    OPEN INPUT MANIFESTFILE-INPUT.
    OPEN OUTPUT MANIFESTFILE-OUTPUT.
    READ MANIFESTFILE-INPUT
        INTO MANIFESTFILE-INPUT-REC
        AT END GO TO PROCESS-MANIFEST-EXIT.
    PERFORM APPEND-ENTRY
        VARYING ENTRY-INDEX FROM 1 BY 1
        UNTIL ENTRY-INDEX > MANIFEST-ADDEDFILESCOUNT + MANIFEST-EXISTINGFILESCOUNT.
PROCESS-MANIFEST-EXIT.
    CLOSE MANIFESTFILE-INPUT.
    CLOSE MANIFESTFILE-OUTPUT.
    EXIT.

APPEND-ENTRY.
    MOVE CLUSTERBY-FUNC(ENTRY-INDEX) TO ENTRY-KEY.
    PERFORM GET-WRITER
        USING ENTRY-KEY, MANIFEST-PARTITIONSPECID.
    CALL "ADDENTRY" USING ENTRY-INDEX, WRITER-OBJECT.
    ADD 1 TO ENTRY-COUNT.

GET-WRITER.
    MOVE PARTITIONSPEC-OBJECT(ARG2) TO WRITER-OBJECT.
    IF WRITER-OBJECT IS NULL
        THEN
            MOVE ARG2 TO WRITER-PARTITIONSPECID
            PERFORM CREATE-WRITER
            MOVE WRITER-OBJECT TO PARTITIONSPEC-OBJECT(ARG2).

CREATE-WRITER.
    OPEN OUTPUT MANIFESTFILE-OUTPUT.
    MOVE 0 TO WRITER-LENGTH.
    PERFORM UNTIL WRITER-LENGTH >= MANIFEST-TARGET-SIZE-BYTES
        PERFORM WRITE-ENTRY
        ADD 1 TO WRITER-LENGTH
    END-PERFORM.
    CLOSE MANIFESTFILE-OUTPUT.
    ADD 1 TO NEW-MANIFESTS.

WRITE-ENTRY.
    WRITE MANIFESTFILE-OUTPUT-REC FROM MANIFESTFILE-INPUT-REC.

VALIDATE-FILES-COUNTS.
    MOVE 0 TO CREATED-FILES-COUNT.
    MOVE 0 TO REPLACED-FILES-COUNT.
    READ MANIFESTFILE-INPUT
        INTO MANIFESTFILE-INPUT-REC
        AT END GO TO VALIDATE-FILES-COUNTS-EXIT.
    IF MANIFEST-CONTENT = 'A' OR MANIFEST-CONTENT = 'R'
        THEN
            ADD MANIFEST-ADDEDFILESCOUNT TO CREATED-FILES-COUNT
            ADD MANIFEST-EXISTINGFILESCOUNT TO CREATED-FILES-COUNT
    ELSE
        ADD MANIFEST-ADDEDFILESCOUNT TO REPLACED-FILES-COUNT
        ADD MANIFEST-EXISTINGFILESCOUNT TO REPLACED-FILES-COUNT.
    GO TO VALIDATE-FILES-COUNTS.
VALIDATE-FILES-COUNTS-EXIT.
    IF CREATED-FILES-COUNT <> REPLACED-FILES-COUNT
        THEN
            DISPLAY 'Replaced and created manifests must have the same number of active files: ' CREATED-FILES-COUNT ' (new), ' REPLACED-FILES-COUNT ' (old)'
            STOP RUN.

CLEAN-UNCOMMITTED.
    PERFORM CLEAN-UNCOMMITTED-MANIFESTS
        USING NEW-MANIFESTS.
    PERFORM CLEAN-UNCOMMITTED-MANIFESTS
        USING REWRITTEN-ADDED-MANIFESTS.

CLEAN-UNCOMMITTED-MANIFESTS.
    READ ARG1
        INTO MANIFESTFILE-INPUT-REC
        AT END GO TO CLEAN-UNCOMMITTED-MANIFESTS-EXIT.
    IF MANIFEST-PATH NOT IN COMMITTED-MANIFESTS
        THEN
            CALL "DELETEFILE" USING MANIFEST-PATH.
    GO TO CLEAN-UNCOMMITTED-MANIFESTS.
CLEAN-UNCOMMITTED-MANIFESTS-EXIT.
    EXIT.

RESET.
    MOVE 0 TO DELETED-MANIFESTS.
    MOVE 0 TO ADDED-MANIFESTS. 
    MOVE 0 TO REWRITTEN-ADDED-MANIFESTS.
    MOVE 0 TO KEPT-MANIFESTS.
    MOVE 0 TO NEW-MANIFESTS.
    MOVE 0 TO REWRITTEN-MANIFESTS.
    MOVE 0 TO ENTRY-COUNT.

MATCH-PREDICATE.
    IF PREDICATE-FUNC IS NULL
        THEN
            RETURN TRUE
    ELSE
        CALL "TESTPREDICATE" USING ARG1, ARG2
        RETURN FUNCTION-RESULT.

CLOSE-WRITERS.
    PERFORM VARYING WRITER-INDEX FROM 1 BY 1
        UNTIL WRITER-INDEX > COUNT OF PARTITIONSPEC-OBJECT
        CALL "CLOSEWRITER" USING PARTITIONSPEC-OBJECT(WRITER-INDEX).