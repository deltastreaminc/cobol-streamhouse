IDENTIFICATION DIVISION.
PROGRAM-ID. PROJECTIONS.

ENVIRONMENT DIVISION.
REPOSITORY.
    COPY PartitionField FROM org/apache/iceberg/PartitionField.
    COPY PartitionSpec FROM org/apache/iceberg/PartitionSpec.
    COPY Transform FROM org/apache/iceberg/transforms/Transform.
    COPY UnboundPredicate FROM org/apache/iceberg/expressions/UnboundPredicate.
    COPY BoundPredicate FROM org/apache/iceberg/expressions/BoundPredicate.
    COPY Expression FROM org/apache/iceberg/expressions/Expression.
    COPY Expressions FROM org/apache/iceberg/expressions/Expressions.
    COPY ExpressionVisitors FROM org/apache/iceberg/expressions/ExpressionVisitors.
    COPY RewriteNot FROM org/apache/iceberg/expressions/RewriteNot.

DATA DIVISION.
WORKING-STORAGE SECTION.

01 WS-SPEC PIC X(40) VALUE SPACE.
01 WS-CASE-SENSITIVE PIC X(1) VALUE SPACE.

PROCEDURE DIVISION.

INCLUSIVE-PROJECTION.
    MOVE PartitionSpec TO WS-SPEC.
    MOVE "T" TO WS-CASE-SENSITIVE.
    PERFORM INCLUSIVE-PROJECTION-PARAGRAPH.

INCLUSIVE-PROJECTION-PARAGRAPH.
    CALL "inclusive" USING WS-SPEC, WS-CASE-SENSITIVE
    RETURNING BaseProjectionEvaluator.

STRICT-PROJECTION.
    MOVE PartitionSpec TO WS-SPEC.
    MOVE "T" TO WS-CASE-SENSITIVE.
    PERFORM STRICT-PROJECTION-PARAGRAPH.

STRICT-PROJECTION-PARAGRAPH.
    CALL "strict" USING WS-SPEC, WS-CASE-SENSITIVE
    RETURNING BaseProjectionEvaluator.

STOP RUN.