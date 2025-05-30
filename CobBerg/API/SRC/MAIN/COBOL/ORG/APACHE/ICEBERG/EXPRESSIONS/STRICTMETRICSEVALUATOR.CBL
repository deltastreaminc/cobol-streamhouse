IDENTIFICATION DIVISION.
PROGRAM-ID. STRICT-METRICS-EVALUATOR.

ENVIRONMENT DIVISION.
REPOSITORY.
    COPY 'ICEBERG-TYPES.cpy'.

DATA DIVISION.
WORKING-STORAGE SECTION.
01 WS-STRUCT                 USAGE POINTER.
01 WS-EXPR                   USAGE POINTER.
01 WS-VALUE-COUNTS           USAGE POINTER.
01 WS-NULL-COUNTS            USAGE POINTER.
01 WS-NAN-COUNTS             USAGE POINTER.
01 WS-LOWER-BOUNDS           USAGE POINTER.
01 WS-UPPER-BOUNDS           USAGE POINTER.
01 WS-ROWS-MUST-MATCH        PIC X(1) VALUE 'T'.
01 WS-ROWS-MIGHT-NOT-MATCH   PIC X(1) VALUE 'F'.

PROCEDURE DIVISION.

MAIN-PROCEDURE.
    PERFORM INIT-EVALUATOR.
    PERFORM EVAL-CONTENT-FILE.
    STOP RUN.

INIT-EVALUATOR.
    MOVE SCHEMA-AS-STRUCT TO WS-STRUCT.
    MOVE REWRITE-NOT(UNBOUND-EXPR) TO WS-EXPR.
    PERFORM BIND-EXPRESSION.

BIND-EXPRESSION.
    CALL 'BINDER-BIND' USING WS-STRUCT, WS-EXPR, CASE-SENSITIVE FLAG.

EVAL-CONTENT-FILE.
    CALL 'EVAL' USING CONTENT-FILE RETURNING WS-RESULT.

HANDLE-NON-REFERENCE.
    RETURN WS-ROWS-MIGHT-NOT-MATCH.

ALWAYS-TRUE.
    RETURN WS-ROWS-MUST-MATCH.

ALWAYS-FALSE.
    RETURN WS-ROWS-MIGHT-NOT-MATCH.

NOT.
    RETURN FUNCTION NOT(WS-RESULT).

AND.
    RETURN FUNCTION AND(LEFT-RESULT, RIGHT-RESULT).

OR.
    RETURN FUNCTION OR(LEFT-RESULT, RIGHT-RESULT).

IS-NULL.
    PERFORM CHECK-NESTED-COLUMN.
    PERFORM CHECK-NULL-VALUES.
    RETURN WS-ROWS-MIGHT-NOT-MATCH.

NOT-NULL.
    PERFORM CHECK-NESTED-COLUMN.
    PERFORM CHECK-NULL-VALUES.
    RETURN WS-ROWS-MIGHT-NOT-MATCH.

IS-NAN.
    PERFORM CHECK-NAN-VALUES.
    RETURN WS-ROWS-MUST-MATCH.

NOT-NAN.
    PERFORM CHECK-NAN-VALUES.
    PERFORM CHECK-NULL-VALUES.
    RETURN WS-ROWS-MIGHT-NOT-MATCH.

LT.
    PERFORM CHECK-NESTED-COLUMN.
    PERFORM CHECK-NULL-VALUES.
    PERFORM CHECK-NAN-VALUES.
    PERFORM CHECK-UPPER-BOUND.
    RETURN WS-ROWS-MIGHT-NOT_MATCH.

LT-EQ.
    PERFORM CHECK-NESTED-COLUMN.
    PERFORM CHECK-NULL-VALUES.
    PERFORM CHECK-NAN-VALUES.
    PERFORM CHECK-UPPER-BOUND.
    RETURN WS-ROWS-MIGHT-NOT-MATCH.

GT.
    PERFORM CHECK-NESTED-COLUMN.
    PERFORM CHECK-NULL-VALUES.
    PERFORM CHECK-NAN-VALUES.
    PERFORM CHECK-LOWER-BOUND.
    RETURN WS-ROWS-MIGHT-NOT-MATCH.

GT-EQ.
    PERFORM CHECK-NESTED-COLUMN.
    PERFORM CHECK-NULL-VALUES.
    PERFORM CHECK-NAN-VALUES.
    PERFORM CHECK-LOWER-BOUND.
    RETURN WS-ROWS-MIGHT-NOT-MATCH.

EQ.
    PERFORM CHECK-NESTED-COLUMN.
    PERFORM CHECK-NULL-VALUES.
    PERFORM CHECK-NAN-VALUES.
    PERFORM CHECK-BOUNDS.
    RETURN WS-ROWS-MUST-MATCH.

NOT-EQ.
    PERFORM CHECK-NESTED-COLUMN.
    PERFORM CHECK-NULL-VALUES.
    PERFORM CHECK-NAN-VALUES.
    PERFORM CHECK-BOUNDS.
    RETURN WS-ROWS-MIGHT-NOT-MATCH.

IN.
    PERFORM CHECK-NESTED-COLUMN.
    PERFORM CHECK-NULL-VALUES.
    PERFORM CHECK-NAN-VALUES.
    PERFORM CHECK-BOUNDS.
    RETURN WS-ROWS-MUST-MATCH.

NOT-IN.
    PERFORM CHECK-NESTED-COLUMN.
    PERFORM CHECK-NULL-VALUES.
    PERFORM CHECK-NAN-VALUES.
    PERFORM CHECK-BOUNDS.
    RETURN WS-ROWS-MUST-MATCH.

START-WITH.
    RETURN WS-ROWS-MIGHT-NOT-MATCH.

NOT-START-WITH.
    RETURN WS-ROWS-MIGHT-NOT-MATCH.

CHECK-NESTED-COLUMN.
    IF NESTED-COLUMN THEN
        RETURN WS-ROWS-MIGHT-NOT-MATCH.

CHECK-NULL-VALUES.
    IF CONTAINS-NULLS THEN
        RETURN WS-ROWS-MIGHT-NOT-MATCH.
    IF NULLS-ONLY THEN
        RETURN WS-ROWS-MUST-MATCH.

CHECK-NAN-VALUES.
    IF CONTAINS-NANS THEN
        RETURN WS-ROWS-MIGHT-NOT-MATCH.
    IF NANS-ONLY THEN
        RETURN WS-ROWS-MUST-MATCH.

CHECK-UPPER-BOUND.
    IF UPPER-BOUND-EXISTS AND UPPER-BOUND-LESS-THAN-LITERAL THEN
        RETURN WS-ROWS-MUST-MATCH.

CHECK-LOWER-BOUND.
    IF LOWER-BOUND-EXISTS AND LOWER-BOUND-GREATER-THAN-LITERAL THEN
        RETURN WS-ROWS-MUST-MATCH.

CHECK-BOUNDS.
    IF LOWER-BOUND-EXISTS AND UPPER-BOUND-EXISTS AND LOWER-BOUND-EQUALS-LITERAL AND UPPER-BOUND-EQUALS-LITERAL THEN
        RETURN WS-ROWS-MUST-MATCH.

NESTED-COLUMN.
    RETURN STRUCT-FIELD-IS-NULL.

CONTAINS-NULLS.
    RETURN NULL-COUNTS-EXIST AND NULL-COUNTS-GREATER-THAN-ZERO.

NULLS-ONLY.
    RETURN VALUE-COUNTS-EXIST AND NULL-COUNTS-EXIST AND VALUE-COUNTS-MINUS-NULL-COUNTS-EQUALS-ZERO.

CONTAINS-NANS.
    RETURN NAN-COUNTS-EXIST AND NAN-COUNTS-GREATER-THAN-ZERO.

NANS-ONLY.
    RETURN NAN-COUNTS-EXIST AND NAN-COUNTS-EQUALS-VALUE-COUNTS.

UPPER-BOUND-EXISTS.
    RETURN UPPER-BOUNDS-EXIST AND UPPER-BOUNDS-CONTAIN-FIELD.

UPPER-BOUND-LESS-THAN-LITERAL.
    RETURN UPPER-BOUND-LESS-THAN-LITERAL-VALUE.

LOWER-BOUND-EXISTS.
    RETURN LOWER-BOUNDS-EXIST AND LOWER-BOUNDS-CONTAIN-FIELD.

LOWER-BOUND-GREATER-THAN-LITERAL.
    RETURN LOWER-BOUND-GREATER-THAN-LITERAL-VALUE.

LOWER-BOUND-EQUALS-LITERAL.
    RETURN LOWER-BOUND-EQUALS-LITERAL-VALUE.

UPPER-BOUND-EQUALS-LITERAL.
    RETURN UPPER-BOUND-EQUALS-LITERAL-VALUE.

STRUCT-FIELD-IS-NULL.
    PERFORM COPY 'ICEBERG-TYPES.cpy'.
    RETURN STRUCT-FIELD(FIELD-ID) IS NULL.

NULL-COUNTS-EXIST.
    RETURN WS-NULL-COUNTS NOT EQUAL TO NULL.

NULL-COUNTS-GREATER-THAN-ZERO.
    RETURN NULL-COUNTS(FIELD-ID) GREATER THAN ZERO.

VALUE-COUNTS-EXIST.
    RETURN WS-VALUE-COUNTS NOT EQUAL TO NULL.

NAN-COUNTS-EXIST.
    RETURN WS-NAN-COUNTS NOT EQUAL TO NULL.

NAN-COUNTS-GREATER-THAN-ZERO.
    RETURN NAN-COUNTS(FIELD-ID) GREATER THAN ZERO.

NAN-COUNTS-EQUALS-VALUE-COUNTS.
    RETURN NAN-COUNTS(FIELD-ID) EQUALS VALUE-COUNTS(FIELD-ID).

UPPER-BOUNDS-EXIST.
    RETURN WS-UPPER-BOUNDS NOT EQUAL TO NULL.

UPPER-BOUNDS-CONTAIN-FIELD.
    RETURN UPPER-BOUNDS CONTAINS FIELD-ID.

UPPER-BOUND-LESS-THAN-LITERAL-VALUE.
    MOVE UPPER-BOUNDS(FIELD-ID) TO UPPER-BOUND.
    RETURN UPPER-BOUND LESS THAN LITERAL-VALUE.

LOWER-BOUNDS-EXIST.
    RETURN WS-LOWER-BOUNDS NOT EQUAL TO NULL.

LOWER-BOUNDS-CONTAIN-FIELD.
    RETURN LOWER-BOUNDS CONTAINS FIELD-ID.

LOWER-BOUND-GREATER-THAN-LITERAL-VALUE.
    MOVE LOWER-BOUNDS(FIELD-ID) TO LOWER-BOUND.
    RETURN LOWER-BOUND GREATER THAN LITERAL-VALUE.

LOWER-BOUND-EQUALS-LITERAL-VALUE.
    MOVE LOWER-BOUNDS(FIELD-ID) TO LOWER-BOUND.
    RETURN LOWER-BOUND EQUALS LITERAL-VALUE.

UPPER-BOUND-EQUALS-LITERAL-VALUE.
    MOVE UPPER-BOUNDS(FIELD-ID) TO UPPER-BOUND.
    RETURN UPPER-BOUND EQUALS LITERAL-VALUE.