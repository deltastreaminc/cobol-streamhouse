IDENTIFICATION DIVISION.
PROGRAM-ID. BASE-UPDATE-PARTITION-SPEC.

ENVIRONMENT DIVISION.
REPOSITORY.
    COPY "ICEBERG-EXCEPTIONS.cob".
    COPY "ICEBERG-EXPRESSIONS.cob".
    COPY "ICEBERG-TRANSFORMS.cob".
    COPY "ICEBERG-TYPES.cob".
    COPY "ICEBERG-UTIL.cob".

DATA DIVISION.
WORKING-STORAGE SECTION.
01 WS-OPS                  OBJECT REFERENCE.
01 WS-BASE                 OBJECT REFERENCE.
01 WS-FORMAT-VERSION       PIC 9(9) BINARY.
01 WS-SPEC                 OBJECT REFERENCE.
01 WS-SCHEMA               OBJECT REFERENCE.
01 WS-NAME-TO-FIELD        OBJECT REFERENCE.
01 WS-TRANSFORM-TO-FIELD   OBJECT REFERENCE.
01 WS-ADDS                 OBJECT REFERENCE.
01 WS-ADDED-TIME-FIELDS    OBJECT REFERENCE.
01 WS-TRANSFORM-TO-ADDED-FIELD OBJECT REFERENCE.
01 WS-NAME-TO-ADDED-FIELD  OBJECT REFERENCE.
01 WS-DELETES              OBJECT REFERENCE.
01 WS-RENAMES              OBJECT REFERENCE.
01 WS-CASE-SENSITIVE       PIC X(1) VALUE "Y".
01 WS-SET-AS-DEFAULT       PIC X(1) VALUE "Y".
01 WS-LAST-ASSIGNED-PARTITION-ID PIC 9(9) BINARY.

PROCEDURE DIVISION.
    PERFORM INIT-OBJECTS.

    PERFORM ASSIGN-FIELD-ID.

    PERFORM RECYCLE-OR-CREATE-PARTITION-FIELD.

    PERFORM CASE-SENSITIVE.
    PERFORM ADD-NON-DEFAULT-SPEC.
    PERFORM ADD-FIELD-BY-SOURCE-NAME.
    PERFORM ADD-FIELD-BY-TERM.
    PERFORM ADD-FIELD-BY-NAME-AND-TERM.
    PERFORM REWRITE-DELETE-AND-ADD-FIELD.
    PERFORM REMOVE-FIELD-BY-NAME.
    PERFORM REMOVE-FIELD-BY-TERM.
    PERFORM RENAME-FIELD.
    PERFORM APPLY.
    PERFORM COMMIT.

    PERFORM RESOLVE.
    PERFORM CHECK-FOR-REDUNDANT-ADDED-PARTITIONS.
    PERFORM INDEX-SPEC-BY-NAME.
    PERFORM INDEX-SPEC-BY-TRANSFORM.
    PERFORM IS-TIME-TRANSFORM.
    PERFORM IS-VOID-TRANSFORM.
    PERFORM PARTITION-NAME-GENERATOR.

    STOP RUN.

INIT-OBJECTS.
    MOVE FUNCTION OBJECT-REFERENCE("org.apache.iceberg.BaseUpdatePartitionSpec") TO WS-OPS.
    MOVE FUNCTION OBJECT-REFERENCE("org.apache.iceberg.TableMetadata") TO WS-BASE.
    MOVE 2 TO WS-FORMAT-VERSION.
    MOVE FUNCTION OBJECT-REFERENCE("org.apache.iceberg.PartitionSpec") TO WS-SPEC.
    MOVE FUNCTION OBJECT-REFERENCE("org.apache.iceberg.Schema") TO WS-SCHEMA.
    MOVE FUNCTION OBJECT-REFERENCE("java.util.Map") TO WS-NAME-TO-FIELD.
    MOVE FUNCTION OBJECT-REFERENCE("java.util.Map") TO WS-TRANSFORM-TO-FIELD.
    MOVE FUNCTION OBJECT-REFERENCE("java.util.List") TO WS-ADDS.
    MOVE FUNCTION OBJECT-REFERENCE("java.util.Map") TO WS-ADDED-TIME-FIELDS.
    MOVE FUNCTION OBJECT-REFERENCE("java.util.Map") TO WS-TRANSFORM-TO-ADDED-FIELD.
    MOVE FUNCTION OBJECT-REFERENCE("java.util.Map") TO WS-NAME-TO-ADDED-FIELD.
    MOVE FUNCTION OBJECT-REFERENCE("java.util.Set") TO WS-DELETES.
    MOVE FUNCTION OBJECT-REFERENCE("java.util.Map") TO WS-RENAMES.

ASSIGN-FIELD-ID.
    ADD 1 TO WS-LAST-ASSIGNED-PARTITION-ID.
    MOVE WS-LAST-ASSIGNED-PARTITION-ID TO RETURN-CODE.

RECYCLE-OR-CREATE-PARTITION-FIELD.
    IF WS-FORMAT-VERSION >= 2 AND WS-BASE NOT EQUAL ZERO
        PERFORM RECYCLE-PARTITION-FIELD
    ELSE
        PERFORM CREATE-PARTITION-FIELD.

RECYCLE-PARTITION-FIELD.
    MOVE FUNCTION OBJECT-REFERENCE("org.apache.iceberg.util.Pair") TO WORK-OBJECT.
    CALL "make" USING WORK-OBJECT, FIRST-ARG, SECOND-ARG, RESULT.
    MOVE RESULT TO WORK-OBJECT.

    PERFORM VARYING PART-SPEC IN WS-BASE
        WHEN PART-SPEC NOT EQUAL ZERO
            PERFORM VARYING PART-FIELD IN PART-SPEC
                WHEN PART-FIELD NOT EQUAL ZERO
                    IF PART-FIELD-SOURCE-ID EQUAL FIRST(WORK-OBJECT)
                        AND PART-FIELD-TRANSFORM EQUAL SECOND(WORK-OBJECT)
                        IF NAME-ARG EQUAL ZERO 
                            OR PART-FIELD-NAME EQUAL NAME-ARG
                            MOVE PART-FIELD TO RETURN-CODE
                            RETURN
                        END-IF
                    END-IF
            END-PERFORM
    END-PERFORM.

    PERFORM CREATE-PARTITION-FIELD.

CREATE-PARTITION-FIELD.
    MOVE FUNCTION OBJECT-REFERENCE("org.apache.iceberg.PartitionField") TO WORK-OBJECT.
    CALL "make" USING WORK-OBJECT, FIRST-ARG, ASSIGN-FIELD-ID(), NAME-ARG, SECOND-ARG, RESULT.
    MOVE RESULT TO RETURN-CODE.

CASE-SENSITIVE.
    MOVE INPUT-PARAMETER(1) TO WS-CASE-SENSITIVE.

ADD-NON-DEFAULT-SPEC.
    MOVE "N" TO WS-SET-AS-DEFAULT.

ADD-FIELD-BY-SOURCE-NAME.
    PERFORM ADD-FIELD-BY-TERM USING FUNCTION OBJECT-REFERENCE("org.apache.iceberg.expressions.Expressions.ref(java.lang.String)"), INPUT-PARAMETER(1).

ADD-FIELD-BY-TERM.
    PERFORM ADD-FIELD-BY-NAME-AND-TERM USING ZERO, INPUT-PARAMETER(1).

ADD-FIELD-BY-NAME-AND-TERM.
    MOVE FUNCTION OBJECT-REFERENCE("org.apache.iceberg.expressions.UnboundTerm") TO WORK-OBJECT.
    CALL "cast" USING INPUT-PARAMETER(2), RESULT.
    PERFORM RESOLVE USING RESULT.
    PERFORM REWRITE-DELETE-AND-ADD-FIELD USING INPUT-PARAMETER(1).
    PERFORM CHECK-FOR-REDUNDANT-ADDED-PARTITIONS USING RESULT.
    MOVE RESULT TO WS-TRANSFORM-TO-ADDED-FIELD.
    MOVE RESULT TO WS-NAME-TO-ADDED-FIELD.
    MOVE RESULT TO WS-ADDS.
    RETURN.

REWRITE-DELETE-AND-ADD-FIELD.
    MOVE WS-DELETES TO WORK-OBJECT.
    CALL "remove" USING WORK-OBJECT, INPUT-PARAMETER(1)-FIELD-ID, IGNORE-RESULT.
    IF INPUT-PARAMETER(2) EQUAL ZERO 
        OR INPUT-PARAMETER(1)-NAME EQUAL INPUT-PARAMETER(2)
        RETURN
    ELSE
        PERFORM RENAME-FIELD USING INPUT-PARAMETER(1)-NAME, INPUT-PARAMETER(2).
    END-IF.

REMOVE-FIELD-BY-NAME.
    MOVE FUNCTION OBJECT-REFERENCE("org.apache.iceberg.expressions.UnboundTerm") TO WORK-OBJECT.
    CALL "cast" USING INPUT-PARAMETER(1), RESULT.
    PERFORM REMOVE-FIELD-BY-TERM USING RESULT.

REMOVE-FIELD-BY-TERM.
    PERFORM RESOLVE USING INPUT-PARAMETER(1).
    MOVE RESULT TO WORK-OBJECT.
    MOVE RESULT TO WS-DELETES.
    RETURN.

RENAME-FIELD.
    MOVE WS-NAME-TO-FIELD TO WORK-OBJECT.
    CALL "get" USING WORK-OBJECT, INPUT-PARAMETER(2), RESULT.
    IF RESULT NOT EQUAL ZERO AND IS-VOID-TRANSFORM(RESULT)
        PERFORM RENAME-FIELD USING RESULT-NAME, CONCATENATE(RESULT-NAME, "_", RESULT-FIELD-ID).
    END-IF.

    MOVE WS-NAME-TO-ADDED-FIELD TO WORK-OBJECT.
    CALL "get" USING WORK-OBJECT, INPUT-PARAMETER(1), RESULT.
    MOVE ZERO TO RESULT.

    MOVE WS-NAME-TO-FIELD TO WORK-OBJECT.
    CALL "get" USING WORK-OBJECT, INPUT-PARAMETER(1), RESULT.
    IF RESULT NOT EQUAL ZERO AND NOT CONTAINS(WS-DELETES, RESULT-FIELD-ID)
        MOVE INPUT-PARAMETER(2) TO WS-RENAMES(INPUT-PARAMETER(1)).
    END-IF.
    RETURN.

APPLY.
    MOVE FUNCTION OBJECT-REFERENCE("org.apache.iceberg.PartitionSpec$Builder") TO WORK-OBJECT.
    CALL "builderFor" USING WS-SCHEMA, RESULT.

    PERFORM VARYING PART-FIELD IN WS-SPEC
        WHEN PART-FIELD NOT EQUAL ZERO
            IF NOT CONTAINS(WS-DELETES, PART-FIELD-FIELD-ID)
                MOVE WS-RENAMES TO WORK-OBJECT.
                CALL "get" USING WORK-OBJECT, PART-FIELD-NAME, NEW-NAME.
                IF NEW-NAME NOT EQUAL ZERO
                    CALL "add" USING RESULT, PART-FIELD-SOURCE-ID, PART-FIELD-FIELD-ID, NEW-NAME, PART-FIELD-TRANSFORM.
                ELSE
                    CALL "add" USING RESULT, PART-FIELD-SOURCE-ID, PART-FIELD-FIELD-ID, PART-FIELD-NAME, PART-FIELD-TRANSFORM.
                END-IF.
            ELSE IF WS-FORMAT-VERSION < 2
                MOVE WS-RENAMES TO WORK-OBJECT.
                CALL "get" USING WORK-OBJECT, PART-FIELD-NAME, NEW-NAME.
                IF NEW-NAME NOT EQUAL ZERO
                    CALL "add" USING RESULT, PART-FIELD-SOURCE-ID, PART-FIELD-FIELD-ID, NEW-NAME, FUNCTION OBJECT-REFERENCE("org.apache.iceberg.transforms.Transforms.alwaysNull()").
                ELSE
                    CALL "add" USING RESULT, PART-FIELD-SOURCE-ID, PART-FIELD-FIELD-ID, PART-FIELD-NAME, FUNCTION OBJECT-REFERENCE("org.apache.iceberg.transforms.Transforms.alwaysNull()").
                END-IF.
            END-IF.
    END-PERFORM.

    PERFORM VARYING NEW-FIELD IN WS-ADDS
        CALL "add" USING RESULT, NEW-FIELD-SOURCE-ID, NEW-FIELD-FIELD-ID, NEW-FIELD-NAME, NEW-FIELD-TRANSFORM.
    END-PERFORM.

    CALL "build" USING RESULT, RETURN-CODE.

COMMIT.
    IF WS-SET-AS-DEFAULT
        MOVE FUNCTION OBJECT-REFERENCE("org.apache.iceberg.TableMetadata") TO WORK-OBJECT.
        CALL "updatePartitionSpec" USING WORK-OBJECT, RETURN-CODE, RESULT.
    ELSE
        MOVE FUNCTION OBJECT-REFERENCE("org.apache.iceberg.TableMetadata") TO WORK-OBJECT.
        CALL "addPartitionSpec" USING WORK-OBJECT, RETURN-CODE, RESULT.
    END-IF.
    CALL "commit" USING WS-OPS, WS-BASE, RESULT.

RESOLVE.
    MOVE FUNCTION OBJECT-REFERENCE("org.apache.iceberg.expressions.UnboundTerm") TO WORK-OBJECT.
    CALL "cast" USING INPUT-PARAMETER(1), RESULT.
    PERFORM BIND-TERM USING RESULT, WS-SCHEMA, WS-CASE-SENSITIVE.
    MOVE RESULT-SOURCE-ID TO FIRST-RESULT.
    PERFORM TO-TRANSFORM USING RESULT, RESULT.
    MOVE RESULT TO SECOND-RESULT.
    MOVE FUNCTION OBJECT-REFERENCE("org.apache.iceberg.util.Pair") TO WORK-OBJECT.
    CALL "make" USING WORK-OBJECT, FIRST-RESULT, SECOND-RESULT, RETURN-CODE.

BIND-TERM.
    MOVE FUNCTION OBJECT-REFERENCE("org.apache.iceberg.expressions.BoundTerm") TO WORK-OBJECT.
    CALL "bind" USING WORK-OBJECT, INPUT-PARAMETER(2), INPUT-PARAMETER(3), RESULT.

TO-TRANSFORM.
    IF FUNCTION OBJECT-REFERENCE("org.apache.iceberg.expressions.BoundReference") EQUAL INPUT-PARAMETER(1)
        MOVE FUNCTION OBJECT-REFERENCE("org.apache.iceberg.transforms.Transforms.identity()") TO RESULT.
    ELSE IF FUNCTION OBJECT-REFERENCE("org.apache.iceberg.expressions.BoundTransform") EQUAL INPUT-PARAMETER(1)
        MOVE RESULT-TRANSFORM TO RESULT.
    ELSE
        MOVE FUNCTION OBJECT-REFERENCE("org.apache.iceberg.exceptions.ValidationException") TO WORK-OBJECT.
        CALL "make" USING WORK-OBJECT, "Invalid term: %s, expected either a bound reference or transform", INPUT-PARAMETER(1), IGNORE-RESULT.
    END-IF.

CHECK-FOR-REDUNDANT-ADDED-PARTITIONS.
    IF IS-TIME-TRANSFORM(INPUT-PARAMETER(1))
        MOVE WS-ADDED-TIME-FIELDS TO WORK-OBJECT.
        CALL "get" USING WORK-OBJECT, INPUT-PARAMETER(1)-SOURCE-ID, TIME-FIELD.
        IF TIME-FIELD NOT EQUAL ZERO
            MOVE FUNCTION OBJECT-REFERENCE("org.apache.iceberg.exceptions.ValidationException") TO WORK-OBJECT.
            CALL "make" USING WORK-OBJECT, "Cannot add redundant partition field: %s conflicts with %s", TIME-FIELD, INPUT-PARAMETER(1), IGNORE-RESULT.
        END-IF.
        MOVE INPUT-PARAMETER(1) TO WS-ADDED-TIME-FIELDS(INPUT-PARAMETER(1)-SOURCE-ID).
    END-IF.

INDEX-SPEC-BY-NAME.
    MOVE FUNCTION OBJECT-REFERENCE("org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap$Builder") TO WORK-OBJECT.
    CALL "builder" USING WORK-OBJECT, RESULT.
    PERFORM VARYING PART-FIELD IN WS-SPEC
        CALL "put" USING RESULT, PART-FIELD-NAME, PART-FIELD, IGNORE-RESULT.
    END-PERFORM.
    CALL "build" USING RESULT, WS-NAME-TO-FIELD.

INDEX-SPEC-BY-TRANSFORM.
    MOVE FUNCTION OBJECT-REFERENCE("java.util.Map") TO