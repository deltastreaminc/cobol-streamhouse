IDENTIFICATION DIVISION.
PROGRAM-ID. JSONUTIL.

ENVIRONMENT DIVISION.
REPOSITORY.
    COPY "JSONPROCESSING".

DATA DIVISION.
WORKING-STORAGE SECTION.
01 WS-FACTORY OBJECT REFERENCE IS JSONPROCESSING.
01 WS-MAPPER OBJECT REFERENCE IS JSONPROCESSING.

PROCEDURE DIVISION.

    DEFINE SUBROUTINE GENERATE.
        PARAMETERS ARE WS-TO-JSON, WS-PRETTY.
        DECLARATIVES.
            01 WS-WRITER OBJECT REFERENCE IS STRINGWRITER.
            01 WS-GENERATOR OBJECT REFERENCE IS JSONGENERATOR.
        END DECLARATIVES.
        OPEN WS-WRITER.
        CREATE WS-GENERATOR FROM WS-FACTORY.
        IF WS-PRETTY = TRUE THEN
            CALL WS-GENERATOR "useDefaultPrettyPrinter".
        END-IF.
        CALL WS-TO-JSON USING WS-GENERATOR.
        CALL WS-GENERATOR "flush".
        RETURN WS-WRITER "toString".
    END SUBROUTINE.

    DEFINE SUBROUTINE PARSE.
        PARAMETERS ARE WS-JSON, WS-PARSER.
        DECLARATIVES.
            01 WS-NODE OBJECT REFERENCE IS JSONNODE.
        END DECLARATIVES.
        CALL WS-MAPPER "readValue" USING WS-JSON, WS-NODE.
        RETURN WS-PARSER USING WS-NODE.
    END SUBROUTINE.

    DEFINE SUBROUTINE GET.
        PARAMETERS ARE WS-PROPERTY, WS-NODE.
        PERFORM PRECONDITION-CHECK THRU PRECONDITION-CHECK-EXIT.
        RETURN WS-NODE "get" USING WS-PROPERTY.
    END SUBROUTINE.

    DEFINE SUBROUTINE GETINT.
        PARAMETERS ARE WS-PROPERTY, WS-NODE.
        PERFORM PRECONDITION-CHECK THRU PRECONDITION-CHECK-EXIT.
        CALL WS-NODE "get" USING WS-PROPERTY RETURNING WS-PROP-NODE.
        PERFORM VALIDATE-INTEGRALNUM THRU VALIDATE-INTEGRALNUM-EXIT.
        RETURN WS-PROP-NODE "asInt".
    END SUBROUTINE.

    DEFINE SUBROUTINE GETINTORNULL.
        PARAMETERS ARE WS-PROPERTY, WS-NODE.
        IF WS-NODE "hasNonNull" USING WS-PROPERTY THEN
            RETURN GETINT USING WS-PROPERTY, WS-NODE.
        ELSE
            RETURN NULL.
        END-IF.
    END SUBROUTINE.

    DEFINE SUBROUTINE GETLONGORNULL.
        PARAMETERS ARE WS-PROPERTY, WS-NODE.
        IF WS-NODE "hasNonNull" USING WS-PROPERTY THEN
            RETURN GETLONG USING WS-PROPERTY, WS-NODE.
        ELSE
            RETURN NULL.
        END-IF.
    END SUBROUTINE.

    DEFINE SUBROUTINE GETLONG.
        PARAMETERS ARE WS-PROPERTY, WS-NODE.
        PERFORM PRECONDITION-CHECK THRU PRECONDITION-CHECK-EXIT.
        CALL WS-NODE "get" USING WS-PROPERTY RETURNING WS-PROP-NODE.
        PERFORM VALIDATE-INTEGRALNUM THRU VALIDATE-INTEGRALNUM-EXIT.
        RETURN WS-PROP-NODE "asLong".
    END SUBROUTINE.

    DEFINE SUBROUTINE GETBOOLORNULL.
        PARAMETERS ARE WS-PROPERTY, WS-NODE.
        IF WS-NODE "hasNonNull" USING WS-PROPERTY THEN
            RETURN GETBOOL USING WS-PROPERTY, WS-NODE.
        ELSE
            RETURN NULL.
        END-IF.
    END SUBROUTINE.

    DEFINE SUBROUTINE GETBOOL.
        PARAMETERS ARE WS-PROPERTY, WS-NODE.
        PERFORM PRECONDITION-CHECK THRU PRECONDITION-CHECK-EXIT.
        CALL WS-NODE "get" USING WS-PROPERTY RETURNING WS-PROP-NODE.
        PERFORM VALIDATE-BOOLEAN THRU VALIDATE-BOOLEAN-EXIT.
        RETURN WS-PROP-NODE "asBoolean".
    END SUBROUTINE.

    DEFINE SUBROUTINE GETSTRING.
        PARAMETERS ARE WS-PROPERTY, WS-NODE.
        PERFORM PRECONDITION-CHECK THRU PRECONDITION-CHECK-EXIT.
        CALL WS-NODE "get" USING WS-PROPERTY RETURNING WS-PROP-NODE.
        PERFORM VALIDATE-TEXTUAL THRU VALIDATE-TEXTUAL-EXIT.
        RETURN WS-PROP-NODE "asText".
    END SUBROUTINE.

    DEFINE SUBROUTINE GETSTRINGORNULL.
        PARAMETERS ARE WS-PROPERTY, WS-NODE.
        IF WS-NODE "has" USING WS-PROPERTY THEN
            CALL WS-NODE "get" USING WS-PROPERTY RETURNING WS-PROP-NODE.
            IF WS-PROP-NODE = NULL THEN
                RETURN NULL.
            ELSE
                RETURN GETSTRING USING WS-PROPERTY, WS-NODE.
            END-IF.
        ELSE
            RETURN NULL.
        END-IF.
    END SUBROUTINE.

    DEFINE SUBROUTINE GETBYTEBUFFERORNULL.
        PARAMETERS ARE WS-PROPERTY, WS-NODE.
        IF WS-NODE "has" USING WS-PROPERTY AND NOT WS-NODE "get" USING WS-PROPERTY "isNull" THEN
            CALL WS-NODE "get" USING WS-PROPERTY RETURNING WS-PROP-NODE.
            PERFORM VALIDATE-TEXTUAL THRU VALIDATE-TEXTUAL-EXIT.
            RETURN BYTEARRAY FROM WS-PROP-NODE "textValue".
        ELSE
            RETURN NULL.
        END-IF.
    END SUBROUTINE.

    DEFINE SUBROUTINE GETSTRINGMAP.
        PARAMETERS ARE WS-PROPERTY, WS-NODE.
        PERFORM PRECONDITION-CHECK THRU PRECONDITION-CHECK-EXIT.
        CALL WS-NODE "get" USING WS-PROPERTY RETURNING WS-PROP-NODE.
        PERFORM VALIDATE-OBJECT THRU VALIDATE-OBJECT-EXIT.
        DECLARE WS-BUILDER OBJECT REFERENCE IS IMMUTABLEMAPBUILDER.
        CALL WS-PROP-NODE "fieldNames" RETURNING WS-FIELD-ITER.
        PERFORM UNTIL WS-FIELD-ITER "hasNext" = FALSE
            CALL WS-FIELD-ITER "next" RETURNING WS-FIELD
            CALL WS-BUILDER "put" USING WS-FIELD, GETSTRING USING WS-FIELD, WS-PROP-NODE
        END-PERFORM.
        RETURN WS-BUILDER "build".
    END SUBROUTINE.

    DEFINE SUBROUTINE GETSTRINGMAPNULLABLEVALUES.
        PARAMETERS ARE WS-PROPERTY, WS-NODE.
        PERFORM PRECONDITION-CHECK THRU PRECONDITION-CHECK-EXIT.
        CALL WS-NODE "get" USING WS-PROPERTY RETURNING WS-PROP-NODE.
        PERFORM VALIDATE-OBJECT THRU VALIDATE-OBJECT-EXIT.
        DECLARE WS-MAP OBJECT REFERENCE IS HASHMAP.
        CALL WS-PROP-NODE "fieldNames" RETURNING WS-FIELD-ITER.
        PERFORM UNTIL WS-FIELD-ITER "hasNext" = FALSE
            CALL WS-FIELD-ITER "next" RETURNING WS-FIELD
            CALL WS-MAP "put" USING WS-FIELD, GETSTRINGORNULL USING WS-FIELD, WS-PROP-NODE
        END-PERFORM.
        RETURN WS-MAP.
    END SUBROUTINE.

    DEFINE SUBROUTINE GETSTRINGARRAY.
        PARAMETERS ARE WS-NODE.
        PERFORM VALIDATE-ARRAY THRU VALIDATE-ARRAY-EXIT.
        DECLARE WS-ARRAY-NODE OBJECT REFERENCE IS ARRAYNODE.
        MOVE WS-ARRAY-NODE TO WS-NODE.
        DECLARE WS-ARRAY DIMENSIONED BY WS-ARRAY-NODE "size".
        PERFORM VARYING WS-I FROM 1 BY 1 UNTIL WS-I > WS-ARRAY-NODE "size"
            MOVE WS-ARRAY-NODE "get" USING WS-I TO WS-ARRAY(WS-I)
        END-PERFORM.
        RETURN WS-ARRAY.
    END SUBROUTINE.

    DEFINE SUBROUTINE GETSTRINGLIST.
        PARAMETERS ARE WS-PROPERTY, WS-NODE.
        PERFORM PRECONDITION-CHECK THRU PRECONDITION-CHECK-EXIT.
        DECLARE WS-BUILDER OBJECT REFERENCE IS IMMUTABLELISTBUILDER.
        CALL WS-BUILDER "addAll" USING NEW JSONSTRINGARRAYITERATOR USING WS-PROPERTY, WS-NODE.
        RETURN WS-BUILDER "build".
    END SUBROUTINE.

    DEFINE SUBROUTINE GETSTRINGSET.
        PARAMETERS ARE WS-PROPERTY, WS-NODE.
        PERFORM PRECONDITION-CHECK THRU PRECONDITION-CHECK-EXIT.
        DECLARE WS-BUILDER OBJECT REFERENCE IS IMMUTABLESETBUILDER.
        CALL WS-BUILDER "addAll" USING NEW JSONSTRINGARRAYITERATOR USING WS-PROPERTY, WS-NODE.
        RETURN WS-BUILDER "build".
    END SUBROUTINE.

    DEFINE SUBROUTINE GETSTRINGLISTORNULL.
        PARAMETERS ARE WS-PROPERTY, WS-NODE.
        IF WS-NODE "has" USING WS-PROPERTY AND NOT WS-NODE "get" USING WS-PROPERTY "isNull" THEN
            DECLARE WS-BUILDER OBJECT REFERENCE IS IMMUTABLELISTBUILDER.
            CALL WS-BUILDER "addAll" USING NEW JSONSTRINGARRAYITERATOR USING WS-PROPERTY, WS-NODE.
            RETURN WS-BUILDER "build".
        ELSE
            RETURN NULL.
        END-IF.
    END SUBROUTINE.

    DEFINE SUBROUTINE GETINTARRAYORNULL.
        PARAMETERS ARE WS-PROPERTY, WS-NODE.
        IF WS-NODE "has" USING WS-PROPERTY AND NOT WS-NODE "get" USING WS-PROPERTY "isNull" THEN
            DECLARE WS-INTEGER-LIST OBJECT REFERENCE IS GETINTEGERLIST USING WS-PROPERTY, WS-NODE.
            RETURN INTARRAY FROM WS-INTEGER-LIST.
        ELSE
            RETURN NULL.
        END-IF.
    END SUBROUTINE.

    DEFINE SUBROUTINE GETINTEGERLIST.
        PARAMETERS ARE WS-PROPERTY, WS-NODE.
        PERFORM PRECONDITION-CHECK THRU PRECONDITION-CHECK-EXIT.
        DECLARE WS-BUILDER OBJECT REFERENCE IS IMMUTABLELISTBUILDER.
        CALL WS-BUILDER "addAll" USING NEW JSONINTEGERARRAYITERATOR USING WS-PROPERTY, WS-NODE.
        RETURN WS-BUILDER "build".
    END SUBROUTINE.

    DEFINE SUBROUTINE GETINTEGERSETORNULL.
        PARAMETERS ARE WS-PROPERTY, WS-NODE.
        IF WS-NODE "hasNonNull" USING WS-PROPERTY THEN
            RETURN GETINTEGERSET USING WS-PROPERTY, WS-NODE.
        ELSE
            RETURN NULL.
        END-IF.
    END SUBROUTINE.

    DEFINE SUBROUTINE GETINTEGERSET.
        PARAMETERS ARE WS-PROPERTY, WS-NODE.
        PERFORM PRECONDITION-CHECK THRU PRECONDITION-CHECK-EXIT.
        DECLARE WS-BUILDER OBJECT REFERENCE IS IMMUTABLESETBUILDER.
        CALL WS-BUILDER "addAll" USING NEW JSONINTEGERARRAYITERATOR USING WS-PROPERTY, WS-NODE.
        RETURN WS-BUILDER "build".
    END SUBROUTINE.

    DEFINE SUBROUTINE GETLONGLIST.
        PARAMETERS ARE WS-PROPERTY, WS-NODE.
        PERFORM PRECONDITION-CHECK THRU PRECONDITION-CHECK-EXIT.
        DECLARE WS-BUILDER OBJECT REFERENCE IS IMMUTABLELISTBUILDER.
        CALL WS-BUILDER "addAll" USING NEW JSONLONGARRAYITERATOR USING WS-PROPERTY, WS-NODE.
        RETURN WS-BUILDER "build".
    END SUBROUTINE.

    DEFINE SUBROUTINE GETLONGLISTORNULL.
        PARAMETERS ARE WS-PROPERTY, WS-NODE.
        IF WS-NODE "has" USING WS-PROPERTY AND NOT WS-NODE "get" USING WS-PROPERTY "isNull" THEN
            DECLARE WS-BUILDER OBJECT REFERENCE IS IMMUTABLELISTBUILDER.
            CALL WS-BUILDER "addAll" USING NEW JSONLONGARRAYITERATOR USING WS-PROPERTY, WS-NODE.
            RETURN WS-BUILDER "build".
        ELSE
            RETURN NULL.
        END-IF.
    END SUBROUTINE.

    DEFINE SUBROUTINE GETLONGSETORNULL.
        PARAMETERS ARE WS-PROPERTY, WS-NODE.
        IF WS-NODE "hasNonNull" USING WS-PROPERTY THEN
            RETURN GETLONGSET USING WS-PROPERTY, WS-NODE.
        ELSE
            RETURN NULL.
        END-IF.
    END SUBROUTINE.

    DEFINE SUBROUTINE GETLONGSET.
        PARAMETERS ARE WS-PROPERTY, WS-NODE.
        PERFORM PRECONDITION-CHECK THRU PRECONDITION-CHECK-EXIT.
        DECLARE WS-BUILDER OBJECT REFERENCE IS IMMUTABLESETBUILDER.
        CALL WS-BUILDER "addAll" USING NEW JSONLONGARRAYITERATOR USING WS-PROPERTY, WS-NODE.
        RETURN WS-BUILDER "build".
    END SUBROUTINE.

    DEFINE SUBROUTINE WRITEINTEGERFIELDF.
        PARAMETERS ARE WS-CONDITION, WS-KEY, WS-VALUE, WS-GENERATOR.
        IF WS-CONDITION THEN
            CALL WS-GENERATOR "writeNumberField" USING WS-KEY, WS-VALUE.
        END-IF.
    END SUBROUTINE.

    DEFINE SUBROUTINE WRITELONGFIELDF.
        PARAMETERS ARE WS-CONDITION, WS-KEY, WS-VALUE, WS-GENERATOR.
        IF WS-CONDITION THEN
            CALL WS-GENERATOR "writeNumberField" USING WS-KEY, WS-VALUE.
        END-IF.
    END SUBROUTINE.

    DEFINE SUBROUTINE WRITEINTEGERARRAY.
        PARAMETERS ARE WS-PROPERTY, WS-ITEMS, WS-GENERATOR.
        CALL WS-GENERATOR "writeArrayFieldStart" USING WS-PROPERTY.
        PERFORM VARYING WS-ITEM IN WS-ITEMS
            CALL WS-GENERATOR "writeNumber" USING WS-ITEM
        END-PERFORM.
        CALL WS-GENERATOR "writeEndArray".
    END