IDENTIFICATION DIVISION.
PROGRAM-ID. BROKER-END-POINT-NOT-AVAILABLE-EXCEPTION.
ENVIRONMENT DIVISION.
DATA DIVISION.
WORKING-STORAGE SECTION.
01 MESSAGE PIC X(256).

PROCEDURE DIVISION.
    ENTRY-POINT.
        IF MESSAGE = SPACES THEN
            MOVE NULL TO MESSAGE
        END-IF.
        MOVE MESSAGE TO EXCEPTION-MESSAGE.
        RAISE RUNTIME-EXCEPTION.
    STOP RUN.