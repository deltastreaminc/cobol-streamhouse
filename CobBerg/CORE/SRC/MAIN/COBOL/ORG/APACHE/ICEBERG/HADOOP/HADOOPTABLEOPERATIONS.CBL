IDENTIFICATION DIVISION.
PROGRAM-ID. HADOOP-TABLE-OPERATIONS.

ENVIRONMENT DIVISION.
    COPY COMMON-CONFIGURATION.

DATA DIVISION.
WORKING-STORAGE SECTION.
    01 CURRENT-METADATA          USAGE POINTER.
    01 VERSION                   PIC 9(10) COMP.
    01 SHOULD-REFRESH            PIC 9 COMP-1 VALUE 1.

    01 VERSION-PATTERN           PIC X(100) VALUE 'v([^\\.]*)\\..*'.
    01 TABLE-METADATA-PARSER-CODEC-VALUES.
        05 FILLER OCCURS 10 TIMES.
           10 CODEC-VALUE         PIC X(50).

    01 TEMP-METADATA-FILE        PIC X(100).
    01 FINAL-METADATA-FILE       PIC X(100).
    01 NEXT-VERSION              PIC 9(10) COMP.
    01 CODEC-NAME                PIC X(100).
    01 CODEC                     PIC X(50).
    01 FILE-EXTENSION            PIC X(20).

    01 TEMP-VERSION-HINT-FILE    PIC X(100).
    01 VERSION-TO-WRITE          PIC 9(10) COMP.

    01 SOURCE-FILE               PIC X(100).
    01 DESTINATION-FILE          PIC X(100).

    COPY LOCK-MANAGER.
    COPY FILE-IO.
    COPY LOCATION-PROVIDER.
    COPY ENCRYPTION-MANAGER.
    COPY PAIR.

PROCEDURE DIVISION.
    MAIN-PARAGRAPH.
        PERFORM CURRENT-OPERATION.
        PERFORM REFRESH-OPERATION.
        PERFORM COMMIT-OPERATION.
        PERFORM TEMP-OPERATION.
        PERFORM GET-METADATA-FILE.
        PERFORM VERSION-HINT-OPERATION.
        PERFORM RENAME-TO-FINAL.
        PERFORM TRY-DELETE.
        PERFORM CHECK-UUID.

    CURRENT-OPERATION.
        IF SHOULD-REFRESH = 1
            PERFORM REFRESH
        ELSE
            RETURN CURRENT-METADATA
        END-IF.

    REFRESH-OPERATION.
        PERFORM FIND-VERSION.
        PERFORM UPDATE-VERSION-AND-METADATA.
        SET SHOULD-REFRESH TO 0.
        RETURN CURRENT-METADATA.

    COMMIT-OPERATION.
        PERFORM GET-CURRENT-VERSION-AND-METADATA.
        IF BASE-METADATA <> CURRENT-METADATA
            RAISE COMMIT-FAILED-EXCEPTION
        END-IF.
        IF BASE-METADATA = METADATA
            RETURN
        END-IF.
        PERFORM VALIDATE-LOCATION-AND-METADATA-LOCATION.
        PERFORM GET-CODEC-AND-FILE-EXTENSION.
        PERFORM CREATE-TEMP-METADATA-FILE.
        PERFORM GET-NEXT-VERSION.
        PERFORM GET-FINAL-METADATA-FILE.
        PERFORM RENAME-TO-FINAL.
        PERFORM WRITE-VERSION-HINT.
        PERFORM DELETE-REMOVED-METADATA-FILES.
        SET SHOULD-REFRESH TO 1.

    TEMP-OPERATION.
        RETURN NEW-TEMPORARY-TABLE-OPERATIONS.

    GET-METADATA-FILE.
        PERFORM ITERATE-CODECS.
        RETURN METADATA-FILE.

    VERSION-HINT-OPERATION.
        PERFORM FIND-VERSION.
        PERFORM WRITE-VERSION-TO-PATH.

    RENAME-TO-FINAL.
        PERFORM ACQUIRE-LOCK.
        PERFORM VERIFY-NO-EXISTING-DESTINATION.
        PERFORM RENAME-FILE.
        PERFORM RELEASE-LOCK.

    TRY-DELETE.
        PERFORM DELETE-FILE.

    CHECK-UUID.
        PERFORM VALIDATE-UUID.
        RETURN NEW-METADATA.

    IDENTIFICATION DIVISION.
    PROGRAM-ID. UTIL.

    ENVIRONMENT DIVISION.
        COPY COMMON-CONFIGURATION.

    DATA DIVISION.
    WORKING-STORAGE SECTION.
        01 VERSION-HINT-FILENAME   PIC X(50) VALUE 'version-hint.metadata'.

    PROCEDURE DIVISION.
        GET-FS.
            RETURN FILESYSTEM-FOR-PATH.

        WRITE-VERSION-TO-PATH.
            OPEN OUTPUT TEMP-VERSION-HINT-FILE
            WRITE VERSION-TO-WRITE
            CLOSE TEMP-VERSION-HINT-FILE
            RENAME TEMP-VERSION-HINT-FILE TO VERSION-HINT-FILENAME.

COPY COMMON-STRUCTURES.
COPY ICEBERG-EXCEPTIONS.