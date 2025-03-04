IDENTIFICATION DIVISION.
PROGRAM-ID. PRUNECOLUMNS.

ENVIRONMENT DIVISION.
REPOSITORY.
    CLASS AVROSCHEMAVISITOR IS org.apache.iceberg.avro.AvroSchemaVisitor.
    CLASS SCHEMA IS org.apache.avro.Schema.
    CLASS FIELD IS org.apache.avro.Schema$Field.
    CLASS LOGICALMAP IS org.apache.avro.LogicalMap.
    CLASS NAMEMAPPING IS org.apache.iceberg.mapping.NameMapping.
    CLASS PRECONDITIONS IS org.apache.iceberg.relocated.com.google.common.base.Preconditions.
    CLASS LISTS IS org.apache.iceberg.relocated.com.google.common.collect.Lists.
    CLASS LOGGER IS org.slf4j.Logger.
    CLASS LOGGERFACTORY IS org.slf4j.LoggerFactory.

DATA DIVISION.
WORKING-STORAGE SECTION.
01 WS-SELECTEDIDS PIC X(18) VALUE "org.apache.iceberg.avro.PruneColumns.selectedIds".
01 WS-NAMEMAPPING PIC X(19) VALUE "org.apache.iceberg.avro.PruneColumns.nameMapping".
01 WS-HASCHANGE PIC 9 VALUE 0.
01 WS-FILTEREDFIELDS PIC X(18) VALUE "org.apache.iceberg.avro.PruneColumns.filteredFields".
01 WS-FIELDNAMES PIC X(17) VALUE "org.apache.iceberg.avro.PruneColumns.fieldNames".
01 WS-PRUNE-LOGGER PIC X(30) VALUE "org.apache.iceberg.avro.PruneColumns".

PROCEDURE DIVISION.
MAIN-PROCEDURE.
    PERFORM ROOTSCHEMA.
    PERFORM RECORD.
    PERFORM UNION.
    PERFORM ARRAY.
    PERFORM MAP.
    PERFORM PRIMITIVE.
    PERFORM COPYRECORD.
    PERFORM ISRECORD.
    PERFORM MAKEEMPTYCOPY.
    PERFORM COPYFIELD.
    PERFORM ISOPTIONSCHEMAWITHNONNULLFIRST.

ROOTSCHEMA.
    MOVE FUNCTION CALL("org.apache.iceberg.avro.AvroSchemaVisitor.visit", WS-SELECTEDIDS, WS-NAMEMAPPING) TO RESULT.
    IF RESULT NOT = NULL THEN
        RETURN RESULT
    END-IF.
    RETURN FUNCTION CALL("org.apache.iceberg.avro.PruneColumns.copyRecord", RECORD, FUNCTION CALL("org.apache.iceberg.relocated.com.google.common.collect.ImmutableList.of")).

RECORD.
    MOVE 0 TO WS-HASCHANGE.
    PERFORM VARYING I FROM 1 BY 1 UNTIL I > LENGTH(RECORD.getFields())
        MOVE FUNCTION CALL("org.apache.iceberg.avro.AvroSchemaUtil.getFieldId", RECORD.getFields(I), WS-NAMEMAPPING, WS-FIELDNAMES) TO FIELDID
        IF FIELDID = NULL THEN
            CONTINUE
        END-IF.
        IF NOT FUNCTION CALL("org.apache.iceberg.avro.AvroSchemaUtil.hasFieldId", RECORD.getFields(I)) THEN
            MOVE 1 TO WS-HASCHANGE
        END-IF.
        IF FUNCTION CALL("org.apache.iceberg.avro.PruneColumns.isOptionSchemaWithNonNullFirstOption", RECORD.getFields(I).schema()) THEN
            MOVE 1 TO WS-HASCHANGE
        END-IF.
        MOVE FUNCTION CALL("org.apache.iceberg.avro.PruneColumns.visit", FIELDS(I).schema(), FIELDS) TO FIELDSCHEMA
        IF WS-SELECTEDIDS CONTAINS FIELDID THEN
            IF FIELDSCHEMA NOT = NULL THEN
                MOVE 1 TO WS-HASCHANGE
                PERFORM COPYFIELD USING RECORD.getFields(I), FIELDSCHEMA, FIELDID
            ELSE
                IF FUNCTION CALL("org.apache.iceberg.avro.PruneColumns.isRecord", RECORD.getFields(I).schema()) THEN
                    MOVE 1 TO WS-HASCHANGE
                    PERFORM COPYFIELD USING RECORD.getFields(I), FUNCTION CALL("org.apache.iceberg.avro.PruneColumns.makeEmptyCopy", RECORD.getFields(I).schema()), FIELDID
                ELSE
                    PERFORM COPYFIELD USING RECORD.getFields(I), RECORD.getFields(I).schema(), FIELDID
                END-IF
            END-IF
        ELSE
            IF FIELDSCHEMA NOT = NULL THEN
                MOVE 1 TO WS-HASCHANGE
                PERFORM COPYFIELD USING RECORD.getFields(I), FIELDSCHEMA, FIELDID
            END-IF
        END-IF
        MOVE FUNCTION CALL("org.apache.iceberg.avro.PruneColumns.copyField", RECORD.getFields(I), FIELDSCHEMA, FIELDID) TO WS-FILTEREDFIELDS(I)
    END-PERFORM.
    IF WS-HASCHANGE = 1 THEN
        RETURN FUNCTION CALL("org.apache.iceberg.avro.PruneColumns.copyRecord", RECORD, WS-FILTEREDFIELDS)
    ELSE
        IF LENGTH(WS-FILTEREDFIELDS) = LENGTH(RECORD.getFields()) THEN
            RETURN RECORD
        ELSE
            IF LENGTH(WS-FILTEREDFIELDS) > 0 THEN
                RETURN FUNCTION CALL("org.apache.iceberg.avro.PruneColumns.copyRecord", RECORD, WS-FILTEREDFIELDS)
            ELSE
                RETURN NULL
            END-IF
        END-IF
    END-IF.

UNION.
    PERFORM VARYING I FROM 1 BY 1 UNTIL I > LENGTH(UNION.getTypes())
        IF UNION.getTypes(I) NOT = NULL THEN
            MOVE UNION.getTypes(I) TO PRUNED
            EXIT PERFORM
        END-IF
    END-PERFORM.
    IF PRUNED NOT = NULL THEN
        IF NOT FUNCTION CALL("java.util.Objects.equals", PRUNED, FUNCTION CALL("org.apache.iceberg.avro.AvroSchemaUtil.fromOption", UNION)) THEN
            RETURN FUNCTION CALL("org.apache.iceberg.avro.AvroSchemaUtil.toOption", PRUNED)
        ELSE
            RETURN UNION
        END-IF
    ELSE
        RETURN NULL
    END-IF.

ARRAY.
    IF ARRAY.getLogicalType() INSTANCE OF LogicalMap THEN
        MOVE FUNCTION CALL("org.apache.iceberg.avro.AvroSchemaUtil.getFieldId", ARRAY.getElementType().getField("key"), WS-NAMEMAPPING, WS-FIELDNAMES) TO KEYID
        MOVE FUNCTION CALL("org.apache.iceberg.avro.AvroSchemaUtil.getFieldId", ARRAY.getElementType().getField("value"), WS-NAMEMAPPING, WS-FIELDNAMES) TO VALUEID
        IF KEYID = NULL OR VALUEID = NULL THEN
            RETURN NULL
        END-IF.
        IF WS-SELECTEDIDS CONTAINS KEYID OR WS-SELECTEDIDS CONTAINS VALUEID THEN
            RETURN FUNCTION CALL("org.apache.iceberg.avro.PruneColumns.complexMapWithIds", ARRAY, KEYID, VALUEID)
        ELSE
            IF ELEMENT NOT = NULL THEN
                MOVE ELEMENT.getField("key") TO KEYPROJECTIONFIELD
                MOVE ELEMENT.getField("value").schema() TO VALUEPROJECTION
                IF KEYPROJECTIONFIELD NOT = NULL AND NOT FUNCTION CALL("java.util.Objects.equals", ARRAY.getElementType().getField("key").schema(), KEYPROJECTIONFIELD.schema()) THEN
                    RETURN FUNCTION CALL("org.apache.iceberg.avro.AvroSchemaUtil.createMap", KEYID, KEYPROJECTIONFIELD.schema(), VALUEID, VALUEPROJECTION)
                ELSE
                    IF NOT FUNCTION CALL("java.util.Objects.equals", ARRAY.getElementType().getField("value").schema(), VALUEPROJECTION) THEN
                        RETURN FUNCTION CALL("org.apache.iceberg.avro.AvroSchemaUtil.createMap", KEYID, ARRAY.getElementType().getField("key").schema(), VALUEID, VALUEPROJECTION)
                    ELSE
                        RETURN FUNCTION CALL("org.apache.iceberg.avro.PruneColumns.complexMapWithIds", ARRAY, KEYID, VALUEID)
                    END-IF
                END-IF
            END-IF
        END-IF
    ELSE
        MOVE FUNCTION CALL("org.apache.iceberg.avro.AvroSchemaUtil.getElementId", ARRAY, WS-NAMEMAPPING, WS-FIELDNAMES) TO ELEMENTID
        IF ELEMENTID = NULL THEN
            RETURN NULL
        END-IF.
        IF WS-SELECTEDIDS CONTAINS ELEMENTID THEN
            RETURN FUNCTION CALL("org.apache.iceberg.avro.PruneColumns.arrayWithId", ARRAY, ELEMENTID)
        ELSE
            IF ELEMENT NOT = NULL THEN
                IF NOT FUNCTION CALL("java.util.Objects.equals", ELEMENT, ARRAY.getElementType()) THEN
                    RETURN FUNCTION CALL("org.apache.iceberg.avro.PruneColumns.arrayWithId", FUNCTION CALL("org.apache.avro.Schema.createArray", ELEMENT), ELEMENTID)
                ELSE
                    RETURN FUNCTION CALL("org.apache.iceberg.avro.PruneColumns.arrayWithId", ARRAY, ELEMENTID)
                END-IF
            ELSE
                RETURN NULL
            END-IF
        END-IF
    END-IF.

MAP.
    MOVE FUNCTION CALL("org.apache.iceberg.avro.AvroSchemaUtil.getKeyId", MAP, WS-NAMEMAPPING, WS-FIELDNAMES) TO KEYID
    MOVE FUNCTION CALL("org.apache.iceberg.avro.AvroSchemaUtil.getValueId", MAP, WS-NAMEMAPPING, WS-FIELDNAMES) TO VALUEID
    IF KEYID = NULL OR VALUEID = NULL THEN
        RETURN NULL
    END-IF.
    IF WS-SELECTEDIDS CONTAINS KEYID OR WS-SELECTEDIDS CONTAINS VALUEID THEN
        RETURN FUNCTION CALL("org.apache.iceberg.avro.PruneColumns.mapWithIds", MAP, KEYID, VALUEID)
    ELSE
        IF VALUE NOT = NULL THEN
            IF NOT FUNCTION CALL("java.util.Objects.equals", VALUE, MAP.getValueType()) THEN
                RETURN FUNCTION CALL("org.apache.iceberg.avro.PruneColumns.mapWithIds", FUNCTION CALL("org.apache.avro.Schema.createMap", VALUE), KEYID, VALUEID)
            ELSE
                RETURN MAP
            END-IF
        ELSE
            RETURN NULL
        END-IF
    END-IF.

PRIMITIVE.
    RETURN NULL.

COPYRECORD.
    MOVE FUNCTION CALL("org.apache.avro.Schema.createRecord", RECORD.getName(), RECORD.getDoc(), RECORD.getNamespace(), RECORD.isError(), NEWFIELDS) TO COPY
    PERFORM VARYING PROP IN RECORD.getObjectProps()
        MOVE PROP.getKey() TO PROPKEY
        MOVE PROP.getValue() TO PROPVALUE
        CALL COPY.addProp(PROPKEY, PROPVALUE)
    END-PERFORM
    RETURN COPY

ISRECORD.
    IF FUNCTION CALL("org.apache.iceberg.avro.AvroSchemaUtil.isOptionSchema", FIELD) THEN
        RETURN FUNCTION CALL("org.apache.iceberg.avro.AvroSchemaUtil.fromOption", FIELD).getType() = SCHEMA.Type.RECORD
    ELSE
        RETURN FIELD.getType() = SCHEMA.Type.RECORD
    END-IF.

MAKEEMPTYCOPY.
    IF FUNCTION CALL("org.apache.iceberg.avro.AvroSchemaUtil.isOptionSchema", FIELD) THEN
        MOVE FUNCTION CALL("org.apache.iceberg.avro.AvroSchemaUtil.fromOption", FIELD) TO INNERSCHEMA
        RETURN FUNCTION CALL("org.apache.avro.Schema.createRecord", INNERSCHEMA.getName(), INNERSCHEMA.getDoc(), INNERSCHEMA.getNamespace(), INNERSCHEMA.isError(), FUNCTION CALL("java.util.Collections.emptyList")) 
    ELSE
        RETURN FUNCTION CALL("org.apache.avro.Schema.createRecord", FIELD.getName(), FIELD.getDoc(), FIELD.getNamespace(), FIELD.isError(), FUNCTION CALL("java.util.Collections.emptyList"))
    END-IF.

COPYFIELD.
    IF FUNCTION CALL("org.apache.iceberg.avro.PruneColumns.isOptionSchemaWithNonNullFirstOption", NEWSCHEMA) THEN
        MOVE FUNCTION CALL("org.apache.iceberg.avro.AvroSchemaUtil.toOption", FUNCTION CALL("org.apache.iceberg.avro.AvroSchemaUtil.fromOption", NEWSCHEMA)) TO NEWSCHEMA-REORDERED
    ELSE
        MOVE NEWSCHEMA TO NEWSCHEMA-REORDERED
    END-IF.
    MOVE FUNCTION CALL("org.apache.avro.Schema$Field", FIELD.name(), NEWSCHEMA-REORDERED, FIELD.doc(), FUNCTION CALL("org.apache.avro.JsonProperties.NULL_VALUE"), FIELD.order()) TO COPY
    PERFORM VARYING PROP IN FIELD.getObjectProps()
        MOVE PROP.getKey() TO PROPKEY
        MOVE PROP.getValue() TO PROPVALUE
        CALL COPY.addProp(PROPKEY, PROPVALUE)
    END-PERFORM
    IF FUNCTION CALL("org.apache.iceberg.avro.AvroSchemaUtil.hasFieldId", FIELD) THEN
        MOVE FUNCTION CALL("org.apache.iceberg.avro.AvroSchemaUtil.getFieldId", FIELD) TO EXISTINGFIELDID
        PERFORM PRECONDITIONS.checkArgument(EXISTINGFIELDID = FIELDID, "Existing field does match with that fetched from name mapping")
    ELSE
        CALL COPY.addProp(FUNCTION CALL("org.apache.iceberg.avro.AvroSchemaUtil.FIELD_ID_PROP"), FIELDID)
    END-IF
    RETURN COPY

ISOPTIONSCHEMAWITHNONNULLFIRST.
    RETURN FUNCTION CALL("org.apache.iceberg.avro.AvroSchemaUtil.isOptionSchema", SCHEMA) AND SCHEMA.getTypes(1).getType() NOT = SCHEMA.Type.NULL.

END PROGRAM.