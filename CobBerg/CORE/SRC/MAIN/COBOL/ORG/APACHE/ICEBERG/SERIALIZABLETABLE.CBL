IDENTIFICATION DIVISION.
PROGRAM-ID. SERIALIZABLETABLE.

ENVIRONMENT DIVISION.
REPOSITORY.
    COPY HadoopConfigurable.

DATA DIVISION.
WORKING-STORAGE SECTION.
01 WS-NAME PIC X(256) VALUE SPACES.
01 WS-LOCATION PIC X(256) VALUE SPACES.
01 WS-METADATAFILELOCATION PIC X(256) VALUE SPACES.
01 WS-PROPERTIES PIC X(1024) VALUE SPACES.
01 WS-SCHEMAASPIC PIC X(1024) VALUE SPACES.
01 WS-DEFAULTSPECID PIC 9(9) COMP VALUE 0.
01 WS-SPECASJSONMAP.
   05 OCCURS 100 TIMES.
      10 WS-SPECID PIC 9(9) COMP.
      10 WS-SPECASJSON PIC X(1024).
01 WS-SORTORDERASJSON PIC X(1024) VALUE SPACES.
01 WS-IO PIC X(128) VALUE SPACES.
01 WS-ENCRYPTION PIC X(128) VALUE SPACES.
01 WS-REFS PIC X(1024) VALUE SPACES.
01 WS-UUID PIC X(36) VALUE SPACES.
01 WS-FORMATVERSION PIC 9(9) COMP VALUE -1.

PROCEDURE DIVISION.

    IDENTIFICATION DIVISION.
    PROGRAM-ID. SERIALIZABLETABLE.

    PROCEDURE DIVISION.
    
        MOVE TABLE-NAME TO WS-NAME.
        MOVE TABLE-LOCATION TO WS-LOCATION.
        MOVE METADATA-FILE-LOCATION TO WS-METADATAFILELOCATION.
        MOVE PROPERTIES TO WS-PROPERTIES.
        MOVE SCHEMA-AS-JSON TO WS-SCHEMAASPIC.
        MOVE DEFAULT-SPEC-ID TO WS-DEFAULTSPECID.
        
        PERFORM VARYING IDX FROM 1 BY 1 UNTIL IDX > 100
            MOVE SPEC-ID(IDX) TO WS-SPECID(IDX)
            MOVE SPEC-AS-JSON(IDX) TO WS-SPECASJSON(IDX)
        END-PERFORM.
        
        MOVE SORTORDER-AS-JSON TO WS-SORTORDERASJSON.
        MOVE FILE-IO TO WS-IO.
        MOVE ENCRYPTION TO WS-ENCRYPTION.
        MOVE REFS TO WS-REFS.
        MOVE UUID TO WS-UUID.
        MOVE FORMAT-VERSION TO WS-FORMATVERSION.

    STOP RUN.

COPY HadoopConfigurable.