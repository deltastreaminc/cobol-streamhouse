IDENTIFICATION DIVISION.
PROGRAM-ID. V2METADATA.

ENVIRONMENT DIVISION.
REPOSITORY.
    COPY "MANIFESTFILE.COPY".
    COPY "DATAFILE.COPY".

DATA DIVISION.
WORKING-STORAGE SECTION.
01 MANIFEST-LIST-SCHEMA.
   COPY "MANIFEST-LIST-SCHEMA.COPY".

01 MANIFEST-FILE-WRAPPER.
   05 COMMIT-SNAPSHOT-ID PIC 9(18) COMP-3.
   05 SEQUENCE-NUMBER PIC 9(18) COMP-3.
   05 WRAPPED-MANIFEST-FILE PIC X(1000).

01 MANIFEST-ENTRY-WRAPPER.
   05 COMMIT-SNAPSHOT-ID PIC 9(18) COMP-3.
   05 DATA-FILE-WRAPPER.
      COPY "DATA-FILE-WRAPPER.COPY".

PROCEDURE DIVISION.

ENTRY-SCHEMA.
    PERFORM ENTRYSCHEMA-PARAGRAPH.

ENTRYSCHEMA-PARAGRAPH.
    MOVE PARTITION-TYPE TO WORK-AREA.
    PERFORM WRAPFILESCHEMA-PARAGRAPH.

WRAPFILESCHEMA-PARAGRAPH.
    MOVE FILE-SCHEMA TO WORK-AREA.
    PERFORM FILETYPE-PARAGRAPH.

FILETYPE-PARAGRAPH.
    MOVE PARTITION-TYPE TO WORK-AREA.
    COPY "FILETYPE.COPY".

MANIFEST-FILE-WRAPPER-PARAGRAPH.
    MOVE COMMIT-SNAPSHOT-ID TO MANIFEST-FILE-WRAPPER.COMMIT-SNAPSHOT-ID.
    MOVE SEQUENCE-NUMBER TO MANIFEST-FILE-WRAPPER.SEQUENCE-NUMBER.
    MOVE WRAPPED-MANIFEST-FILE TO MANIFEST-FILE-WRAPPER.WRAPPED-MANIFEST-FILE.

MANIFEST-ENTRY-WRAPPER-PARAGRAPH.
    MOVE COMMIT-SNAPSHOT-ID TO MANIFEST-ENTRY-WRAPPER.COMMIT-SNAPSHOT-ID.
    MOVE DATA-FILE-WRAPPER TO MANIFEST-ENTRY-WRAPPER.DATA-FILE-WRAPPER.

DATA-FILE-WRAPPER-PARAGRAPH.
    MOVE FILE-SCHEMA TO DATA-FILE-WRAPPER.FILE-SCHEMA.

STOP RUN.