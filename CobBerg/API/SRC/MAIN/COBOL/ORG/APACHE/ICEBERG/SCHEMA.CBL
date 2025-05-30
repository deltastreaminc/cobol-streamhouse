IDENTIFICATION DIVISION.
PROGRAM-ID. ICEBERG-SCHEMA.

ENVIRONMENT DIVISION.
SPECIAL-NAMES.
    EXTERNAL.

DATA DIVISION.
WORKING-STORAGE SECTION.
01 WS-SCHEMA-ID                  PIC 9(10) COMP.
01 WS-HIGHEST-FIELD-ID           PIC 9(10) COMP.
01 WS-IDENTIFIER-FIELD-IDS       PIC 9(10) OCCURS 1 TO 9999 TIMES DEPENDING ON WS-NUM-IDENTIFIER-FIELDS COMP.
01 WS-NUM-IDENTIFIER-FIELDS      PIC 9(5) COMP.
01 WS-ALIASES                    USAGE IS OBJECT REFERENCE.
01 WS-ID-TO-FIELD                USAGE IS OBJECT REFERENCE.
01 WS-NAME-TO-ID                 USAGE IS OBJECT REFERENCE.
01 WS-LOWERCASE-NAME-TO-ID       USAGE IS OBJECT REFERENCE.
01 WS-ID-TO-ACCESSOR              USAGE IS OBJECT REFERENCE.
01 WS-ID-TO-NAME                 USAGE IS OBJECT REFERENCE.
01 WS-IDENTIFIER-FIELD-ID-SET    USAGE IS OBJECT REFERENCE.
01 WS-IDS-TO-REASSIGNED          USAGE IS OBJECT REFERENCE.
01 WS-IDS-TO-ORIGINAL            USAGE IS OBJECT REFERENCE.

PROCEDURE DIVISION.
    PERFORM INITIALIZE-SCHEMA.
    PERFORM VALIDATE-IDENTIFIER-FIELDS.
    PERFORM VALIDATE-SCHEMA-COMPATIBILITY.
    STOP RUN.

INITIALIZE-SCHEMA.
    MOVE WS-SCHEMA-ID TO SCHEMA-ID.
    MOVE WS-HIGHEST-FIELD-ID TO HIGHEST-FIELD-ID.
    MOVE WS-ALIASES TO ALIASES.
    MOVE WS-ID-TO-FIELD TO ID-TO-FIELD.
    MOVE WS-NAME-TO-ID TO NAME-TO-ID.
    MOVE WS-LOWERCASE-NAME-TO-ID TO LOWERCASE-NAME-TO-ID.
    MOVE WS-ID-TO-ACCESSOR TO ID-TO-ACCESSOR.
    MOVE WS-ID-TO-NAME TO ID-TO-NAME.
    MOVE WS-IDENTIFIER-FIELD-ID-SET TO IDENTIFIER-FIELD-ID-SET.
    MOVE WS-IDS-TO-REASSIGNED TO IDS-TO-REASSIGNED.
    MOVE WS-IDS-TO-ORIGINAL TO IDS-TO-ORIGINAL.

VALIDATE-IDENTIFIER-FIELDS.
    PERFORM VARYING I FROM 1 BY 1 UNTIL I > WS-NUM-IDENTIFIER-FIELDS
        MOVE WS-IDENTIFIER-FIELD-IDS(I) TO FIELD-ID
        PERFORM VALIDATE-IDENTIFIER-FIELD
    END-PERFORM.

VALIDATE-IDENTIFIER-FIELD.
    MOVE ID-TO-FIELD(FIELD-ID) TO FIELD.
    PERFORM VALIDATE-FIELD-TYPE.
    PERFORM VALIDATE-FIELD-REQUIRED.
    PERFORM VALIDATE-FIELD-PARENT.

VALIDATE-FIELD-TYPE.
    IF FIELD-TYPE IS NOT PRIMITIVE-TYPE
        RAISE EXCEPTION "Cannot add field %s as an identifier field: not a primitive type field" USING FIELD-NAME.

VALIDATE-FIELD-REQUIRED.
    IF FIELD-IS-OPTIONAL
        RAISE EXCEPTION "Cannot add field %s as an identifier field: not a required field" USING FIELD-NAME.

VALIDATE-FIELD-PARENT.
    MOVE ID-TO-PARENT(FIELD-ID) TO PARENT-ID.
    PERFORM UNTIL PARENT-ID IS NULL
        MOVE ID-TO-FIELD(PARENT-ID) TO PARENT
        IF PARENT-TYPE IS NOT STRUCT-TYPE
            RAISE EXCEPTION "Cannot add field %s as an identifier field: must not be nested in %s" USING FIELD-NAME, PARENT
        END-IF
        IF PARENT-IS-OPTIONAL
            RAISE EXCEPTION "Cannot add field %s as an identifier field: must not be nested in an optional field %s" USING FIELD-NAME, PARENT
        END-IF
        MOVE ID-TO-PARENT(PARENT-ID) TO PARENT-ID
    END-PERFORM.

VALIDATE-SCHEMA-COMPATIBILITY.
    PERFORM VARYING FIELD IN SCHEMA-FIELDS
        MOVE FIELD-TYPE-ID TO TYPE-ID
        PERFORM CHECK-MIN-FORMAT-VERSION
        PERFORM CHECK-DEFAULT-VALUE
    END-PERFORM.

CHECK-MIN-FORMAT-VERSION.
    MOVE MIN-FORMAT-VERSIONS(TYPE-ID) TO MIN-VERSION.
    IF SCHEMA-FORMAT-VERSION < MIN-VERSION
        RAISE EXCEPTION "Invalid type for %s: %s is not supported until v%s" USING FIELD-NAME, FIELD-TYPE, MIN-VERSION
    END-IF.

CHECK-DEFAULT-VALUE.
    IF FIELD-DEFAULT-VALUE IS NOT NULL AND SCHEMA-FORMAT-VERSION < 3
        RAISE EXCEPTION "Invalid initial default for %s: non-null default (%s) is not supported until v3" USING FIELD-NAME, FIELD-DEFAULT-VALUE
    END-IF.