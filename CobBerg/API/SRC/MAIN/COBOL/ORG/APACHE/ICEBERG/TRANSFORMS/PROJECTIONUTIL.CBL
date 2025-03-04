IDENTIFICATION DIVISION.
PROGRAM-ID. PROJECTION-UTIL.

ENVIRONMENT DIVISION.

DATA DIVISION.
WORKING-STORAGE SECTION.
01 WS-BOUNDARY            PIC S9(9) COMP.
01 WS-MINUSONE            PIC S9(9) COMP.
01 WS-PLUSONE             PIC S9(9) COMP.
01 WS-BOUNDARY-DECIMAL    PIC S9(18)V9(18) COMP-3.
01 WS-MINUSONE-DECIMAL    PIC S9(18)V9(18) COMP-3.
01 WS-PLUSONE-DECIMAL     PIC S9(18)V9(18) COMP-3.

PROCEDURE DIVISION.

    ENTRY 'TRUNCATE-INTEGER'.
        MOVE PRED-LITERAL-VALUE TO WS-BOUNDARY.
        EVALUATE PRED-OPERATION
            WHEN 'LT'
                COMPUTE WS-MINUSONE = WS-BOUNDARY - 1
                MOVE WS-MINUSONE TO RESULT
            WHEN 'LT-EQ'
                MOVE WS-BOUNDARY TO RESULT
            WHEN 'GT'
                COMPUTE WS-PLUSONE = WS-BOUNDARY + 1
                MOVE WS-PLUSONE TO RESULT
            WHEN 'GT-EQ'
                MOVE WS-BOUNDARY TO RESULT
            WHEN 'EQ'
                MOVE WS-BOUNDARY TO RESULT
            WHEN OTHER
                MOVE NULL TO RESULT
        END-EVALUATE.
        GOBACK.

    ENTRY 'TRUNCATE-INTEGER-STRICT'.
        MOVE PRED-LITERAL-VALUE TO WS-BOUNDARY.
        EVALUATE PRED-OPERATION
            WHEN 'LT'
                MOVE WS-BOUNDARY TO RESULT
            WHEN 'LT-EQ'
                COMPUTE WS-PLUSONE = WS-BOUNDARY + 1
                MOVE WS-PLUSONE TO RESULT
            WHEN 'GT'
                MOVE WS-BOUNDARY TO RESULT
            WHEN 'GT-EQ'
                COMPUTE WS-MINUSONE = WS-BOUNDARY - 1
                MOVE WS-MINUSONE TO RESULT
            WHEN 'NOT-EQ'
                MOVE WS-BOUNDARY TO RESULT
            WHEN 'EQ'
                MOVE NULL TO RESULT
            WHEN OTHER
                MOVE NULL TO RESULT
        END-EVALUATE.
        GOBACK.

    ENTRY 'TRUNCATE-LONG-STRICT'.
        MOVE PRED-LITERAL-VALUE TO WS-BOUNDARY.
        EVALUATE PRED-OPERATION
            WHEN 'LT'
                MOVE WS-BOUNDARY TO RESULT
            WHEN 'LT-EQ'
                COMPUTE WS-PLUSONE = WS-BOUNDARY + 1
                MOVE WS-PLUSONE TO RESULT
            WHEN 'GT'
                MOVE WS-BOUNDARY TO RESULT
            WHEN 'GT-EQ'
                COMPUTE WS-MINUSONE = WS-BOUNDARY - 1
                MOVE WS-MINUSONE TO RESULT
            WHEN 'NOT-EQ'
                MOVE WS-BOUNDARY TO RESULT
            WHEN 'EQ'
                MOVE NULL TO RESULT
            WHEN OTHER
                MOVE NULL TO RESULT
        END-EVALUATE.
        GOBACK.

    ENTRY 'TRUNCATE-LONG'.
        MOVE PRED-LITERAL-VALUE TO WS-BOUNDARY.
        EVALUATE PRED-OPERATION
            WHEN 'LT'
                COMPUTE WS-MINUSONE = WS-BOUNDARY - 1
                MOVE WS-MINUSONE TO RESULT
            WHEN 'LT-EQ'
                MOVE WS-BOUNDARY TO RESULT
            WHEN 'GT'
                COMPUTE WS-PLUSONE = WS-BOUNDARY + 1
                MOVE WS-PLUSONE TO RESULT
            WHEN 'GT-EQ'
                MOVE WS-BOUNDARY TO RESULT
            WHEN 'EQ'
                MOVE WS-BOUNDARY TO RESULT
            WHEN OTHER
                MOVE NULL TO RESULT
        END-EVALUATE.
        GOBACK.

    ENTRY 'TRUNCATE-DECIMAL'.
        MOVE PRED-LITERAL-VALUE TO WS-BOUNDARY-DECIMAL.
        EVALUATE PRED-OPERATION
            WHEN 'LT'
                COMPUTE WS-MINUSONE-DECIMAL = WS-BOUNDARY-DECIMAL - 0.000000000000000001
                MOVE WS-MINUSONE-DECIMAL TO RESULT
            WHEN 'LT-EQ'
                MOVE WS-BOUNDARY-DECIMAL TO RESULT
            WHEN 'GT'
                COMPUTE WS-PLUSONE-DECIMAL = WS-BOUNDARY-DECIMAL + 0.000000000000000001
                MOVE WS-PLUSONE-DECIMAL TO RESULT
            WHEN 'GT-EQ'
                MOVE WS-BOUNDARY-DECIMAL TO RESULT
            WHEN 'EQ'
                MOVE WS-BOUNDARY-DECIMAL TO RESULT
            WHEN OTHER
                MOVE NULL TO RESULT
        END-EVALUATE.
        GOBACK.

    ENTRY 'TRUNCATE-DECIMAL-STRICT'.
        MOVE PRED-LITERAL-VALUE TO WS-BOUNDARY-DECIMAL.
        COMPUTE WS-MINUSONE-DECIMAL = WS-BOUNDARY-DECIMAL - 0.000000000000000001.
        COMPUTE WS-PLUSONE-DECIMAL = WS-BOUNDARY-DECIMAL + 0.000000000000000001.
        EVALUATE PRED-OPERATION
            WHEN 'LT'
                MOVE WS-BOUNDARY-DECIMAL TO RESULT
            WHEN 'LT-EQ'
                MOVE WS-PLUSONE-DECIMAL TO RESULT
            WHEN 'GT'
                MOVE WS-BOUNDARY-DECIMAL TO RESULT
            WHEN 'GT-EQ'
                MOVE WS-MINUSONE-DECIMAL TO RESULT
            WHEN 'NOT-EQ'
                MOVE WS-BOUNDARY-DECIMAL TO RESULT
            WHEN 'EQ'
                MOVE NULL TO RESULT
            WHEN OTHER
                MOVE NULL TO RESULT
        END-EVALUATE.
        GOBACK.

    ENTRY 'TRUNCATE-ARRAY'.
        MOVE PRED-LITERAL-VALUE TO WS-BOUNDARY.
        EVALUATE PRED-OPERATION
            WHEN 'LT', 'LT-EQ'
                MOVE WS-BOUNDARY TO RESULT
            WHEN 'GT', 'GT-EQ'
                MOVE WS-BOUNDARY TO RESULT
            WHEN 'EQ'
                MOVE WS-BOUNDARY TO RESULT
            WHEN 'STARTS-WITH'
                MOVE WS-BOUNDARY TO RESULT
            WHEN OTHER
                MOVE NULL TO RESULT
        END-EVALUATE.
        GOBACK.

    ENTRY 'TRUNCATE-ARRAY-STRICT'.
        MOVE PRED-LITERAL-VALUE TO WS-BOUNDARY.
        EVALUATE PRED-OPERATION
            WHEN 'LT', 'LT-EQ'
                MOVE WS-BOUNDARY TO RESULT
            WHEN 'GT', 'GT-EQ'
                MOVE WS-BOUNDARY TO RESULT
            WHEN 'NOT-EQ'
                MOVE WS-BOUNDARY TO RESULT
            WHEN 'EQ'
                MOVE NULL TO RESULT
            WHEN OTHER
                MOVE NULL TO RESULT
        END-EVALUATE.
        GOBACK.

    ENTRY 'FIX-INCLUSIVE-TIME-PROJECTION'.
        MOVE PROJ-LITERAL-VALUE TO WS-BOUNDARY.
        EVALUATE PROJ-OPERATION
            WHEN 'LT'
                IF WS-BOUNDARY < 0
                    COMPUTE WS-PLUSONE = WS-BOUNDARY + 1
                    MOVE WS-PLUSONE TO RESULT
                ELSE
                    MOVE WS-BOUNDARY TO RESULT
                END-IF
            WHEN 'LT-EQ'
                IF WS-BOUNDARY < 0
                    COMPUTE WS-PLUSONE = WS-BOUNDARY + 1
                    MOVE WS-PLUSONE TO RESULT
                ELSE
                    MOVE WS-BOUNDARY TO RESULT
                END-IF
            WHEN 'GT', 'GT-EQ'
                MOVE WS-BOUNDARY TO RESULT
            WHEN 'EQ'
                IF WS-BOUNDARY < 0
                    MOVE WS-BOUNDARY TO RESULT-1
                    COMPUTE WS-PLUSONE = WS-BOUNDARY + 1
                    MOVE WS-PLUSONE TO RESULT-2
                    MOVE RESULT-1 TO RESULT
                ELSE
                    MOVE WS-BOUNDARY TO RESULT
                END-IF
            WHEN 'IN'
                PERFORM VARYING I FROM 1 BY 1 UNTIL I > LENGTH OF PROJ-LITERAL-SET
                    MOVE PROJ-LITERAL-SET(I) TO WS-BOUNDARY
                    IF WS-BOUNDARY < 0
                        MOVE WS-BOUNDARY TO RESULT-SET(I)
                        COMPUTE WS-PLUSONE = WS-BOUNDARY + 1
                        MOVE WS-PLUSONE TO RESULT-SET(I + 1)
                    ELSE
                        MOVE WS-BOUNDARY TO RESULT-SET(I)
                    END-IF
                END-PERFORM
                MOVE RESULT-SET TO RESULT
            WHEN OTHER
                MOVE PROJ TO RESULT
        END-EVALUATE.
        GOBACK.

    ENTRY 'FIX-STRICT-TIME-PROJECTION'.
        MOVE PROJ-LITERAL-VALUE TO WS-BOUNDARY.
        EVALUATE PROJ-OPERATION
            WHEN 'LT', 'LT-EQ'
                MOVE PROJ TO RESULT
            WHEN 'GT'
                IF WS-BOUNDARY <= 0
                    COMPUTE WS-PLUSONE = WS-BOUNDARY + 1
                    MOVE WS-PLUSONE TO RESULT
                ELSE
                    MOVE PROJ TO RESULT
                END-IF
            WHEN 'GT-EQ'
                IF WS-BOUNDARY <= 0
                    COMPUTE WS-PLUSONE = WS-BOUNDARY + 1
                    MOVE WS-PLUSONE TO RESULT
                ELSE
                    MOVE PROJ TO RESULT
                END-IF
            WHEN 'EQ', 'IN'
                MOVE NULL TO RESULT
            WHEN 'NOT-EQ'
                IF WS-BOUNDARY < 0
                    MOVE WS-BOUNDARY TO RESULT-1
                    COMPUTE WS-PLUSONE = WS-BOUNDARY + 1
                    MOVE WS-PLUSONE TO RESULT-2
                    MOVE RESULT-1 TO RESULT
                ELSE
                    MOVE PROJ TO RESULT
                END-IF
            WHEN 'NOT-IN'
                PERFORM VARYING I FROM 1 BY 1 UNTIL I > LENGTH OF PROJ-LITERAL-SET
                    MOVE PROJ-LITERAL-SET(I) TO WS-BOUNDARY
                    IF WS-BOUNDARY < 0
                        MOVE WS-BOUNDARY TO RESULT-SET(I)
                        COMPUTE WS-PLUSONE = WS-BOUNDARY + 1
                        MOVE WS-PLUSONE TO RESULT-SET(I + 1)
                    ELSE
                        MOVE WS-BOUNDARY TO RESULT-SET(I)
                    END-IF
                END-PERFORM
                MOVE RESULT-SET TO RESULT
            WHEN OTHER
                MOVE NULL TO RESULT
        END-EVALUATE.
        GOBACK.

STOP RUN.