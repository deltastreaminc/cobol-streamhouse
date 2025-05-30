IDENTIFICATION DIVISION.
PROGRAM-ID. LITERALS.

ENVIRONMENT DIVISION.
SPECIAL-NAMES.
    DECIMAL-POINT IS COMMA.

DATA DIVISION.
WORKING-STORAGE SECTION.
01 EPOCH-OFFSET PIC S9(9) COMP-5 VALUE 0.
01 EPOCH-DAY PIC S9(9) COMP-5.

PROCEDURE DIVISION.

IDENTIFICATION DIVISION.
START-PROGRAM.

    PERFORM GET-LITERAL-VALUE
    DISPLAY "Literal value: " LITERAL-VALUE

    STOP RUN.

GET-LITERAL-VALUE SECTION.
    MOVE ZERO TO LITERAL-VALUE

    EVALUATE TRUE
        WHEN LITERAL-TYPE = 'BOOLEAN'
            MOVE LITERAL-BOOLEAN-VALUE TO LITERAL-VALUE
        WHEN LITERAL-TYPE = 'INTEGER'
            MOVE LITERAL-INTEGER-VALUE TO LITERAL-VALUE
        WHEN LITERAL-TYPE = 'LONG'
            MOVE LITERAL-LONG-VALUE TO LITERAL-VALUE
        WHEN LITERAL-TYPE = 'FLOAT'
            MOVE LITERAL-FLOAT-VALUE TO LITERAL-VALUE
        WHEN LITERAL-TYPE = 'DOUBLE'
            MOVE LITERAL-DOUBLE-VALUE TO LITERAL-VALUE
        WHEN LITERAL-TYPE = 'STRING'
            MOVE LITERAL-STRING-VALUE TO LITERAL-VALUE
        WHEN LITERAL-TYPE = 'UUID'
            MOVE LITERAL-UUID-VALUE TO LITERAL-VALUE
        WHEN LITERAL-TYPE = 'FIXED'
            MOVE LITERAL-FIXED-VALUE TO LITERAL-VALUE
        WHEN LITERAL-TYPE = 'BINARY'
            MOVE LITERAL-BINARY-VALUE TO LITERAL-VALUE
        WHEN LITERAL-TYPE = 'DECIMAL'
            MOVE LITERAL-DECIMAL-VALUE TO LITERAL-VALUE
    END-EVALUATE.

    MOVE EPOCH-OFFSET TO EPOCH-DAY.

EXIT SECTION.