IDENTIFICATION DIVISION.
PROGRAM-ID. SIZE-BASED-FILE-REWRITER.

ENVIRONMENT DIVISION.
INPUT-OUTPUT SECTION.
FILE-CONTROL.

DATA DIVISION.
WORKING-STORAGE SECTION.
01 WS-TARGET-FILE-SIZE           PIC 9(18) COMP.
01 WS-MIN-FILE-SIZE              PIC 9(18) COMP.
01 WS-MAX-FILE-SIZE              PIC 9(18) COMP.
01 WS-MIN-INPUT-FILES            PIC 9(9) COMP.
01 WS-REWRITE-ALL                PIC 9 COMP-X.
01 WS-MAX-GROUP-SIZE             PIC 9(18) COMP.
01 WS-OUTPUT-SPEC-ID             PIC 9(9) COMP.

PROCEDURE DIVISION.

    PERFORM INIT-REWRITER.
    PERFORM PLAN-FILE-GROUPS.

    STOP RUN.

INIT-REWRITER.
    MOVE DEFAULT-TARGET-FILE-SIZE TO WS-TARGET-FILE-SIZE.
    MOVE DEFAULT-MIN-FILE-SIZE TO WS-MIN-FILE-SIZE.
    MOVE DEFAULT-MAX-FILE-SIZE TO WS-MAX-FILE-SIZE.
    MOVE DEFAULT-MIN-INPUT-FILES TO WS-MIN-INPUT-FILES.
    MOVE DEFAULT-REWRITE-ALL TO WS-REWRITE-ALL.
    MOVE DEFAULT-MAX-GROUP-SIZE TO WS-MAX-GROUP-SIZE.
    MOVE DEFAULT-OUTPUT-SPEC-ID TO WS-OUTPUT-SPEC-ID.

PLAN-FILE-GROUPS.
    PERFORM FILTER-FILES.
    PERFORM BIN-PACK-FILES.
    PERFORM FILTER-FILE-GROUPS.

FILTER-FILES.
    IF WS-REWRITE-ALL = 1 THEN
        PERFORM ITERATE-TASKS
    ELSE
        PERFORM CALL-FILTER-FILES
    END-IF.

BIN-PACK-FILES.
    PERFORM BIN-PACK-TASK-LIST.

FILTER-FILE-GROUPS.
    IF WS-REWRITE-ALL = 1 THEN
        PERFORM RETURN-FILE-GROUPS
    ELSE
        PERFORM CALL-FILTER-FILE-GROUPS
    END-IF.

ITERATE-TASKS.
    * Implement logic to iterate through tasks

CALL-FILTER-FILES.
    * Implement logic to call abstract method filterFiles()

BIN-PACK-TASK-LIST.
    * Implement logic to bin-pack tasks into file groups

RETURN-FILE-GROUPS.
    * Implement logic to return file groups

CALL-FILTER-FILE-GROUPS.
    * Implement logic to call abstract method filterFileGroups()

DEFAULT-TARGET-FILE-SIZE.
    * Implement logic to return default target file size

DEFAULT-MIN-FILE-SIZE.
    * Implement logic to return default min file size

DEFAULT-MAX-FILE-SIZE.
    * Implement logic to return default max file size

DEFAULT-MIN-INPUT-FILES.
    * Implement logic to return default min input files

DEFAULT-REWRITE-ALL.
    * Implement logic to return default rewrite all flag

DEFAULT-MAX-GROUP-SIZE.
    * Implement logic to return default max group size

DEFAULT-OUTPUT-SPEC-ID.
    * Implement logic to return default output spec id