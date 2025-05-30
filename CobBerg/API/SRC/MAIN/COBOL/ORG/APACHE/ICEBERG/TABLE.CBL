IDENTIFICATION DIVISION.
PROGRAM-ID. TABLE-INTERFACE.

ENVIRONMENT DIVISION.
INPUT-OUTPUT SECTION.
FILE-CONTROL.

DATA DIVISION.
WORKING-STORAGE SECTION.
01 WS-TABLE-NAME PIC X(50).
01 WS-TABLE-LOCATION PIC X(100).
01 WS-TABLE-PROPERTIES PIC X(1000).
01 WS-TABLE-SNAPSHOTS PIC X(1000).
01 WS-TABLE-HISTORY PIC X(1000).
01 WS-TABLE-SCHEMA PIC X(1000).
01 WS-TABLE-PARTITION-SPEC PIC X(1000).
01 WS-TABLE-SORT-ORDER PIC X(1000).
01 WS-TABLE-CURRENT-SNAPSHOT PIC 9(18).
01 WS-TABLE-SNAPSHOT-BY-ID PIC 9(18).
01 WS-TABLE-SNAPSHOT-BY-NAME PIC X(50).
01 WS-TABLE-UUID PIC X(36).

PROCEDURE DIVISION.

REFRESH-TABLE.
    PERFORM REFRESH-TABLE-METADATA.

REFRESH-TABLE-METADATA.
    MOVE FUNCTION CURRENT-DATE TO WS-TABLE-NAME.
    MOVE FUNCTION RANDOM-REAL TO WS-TABLE-LOCATION.
    MOVE FUNCTION RANDOM-REAL TO WS-TABLE-PROPERTIES.
    MOVE FUNCTION RANDOM-REAL TO WS-TABLE-SNAPSHOTS.
    MOVE FUNCTION RANDOM-REAL TO WS-TABLE-HISTORY.
    MOVE FUNCTION RANDOM-REAL TO WS-TABLE-SCHEMA.
    MOVE FUNCTION RANDOM-REAL TO WS-TABLE-PARTITION-SPEC.
    MOVE FUNCTION RANDOM-REAL TO WS-TABLE-SORT-ORDER.
    MOVE FUNCTION RANDOM-REAL TO WS-TABLE-CURRENT-SNAPSHOT.
    MOVE FUNCTION RANDOM-REAL TO WS-TABLE-SNAPSHOT-BY-ID.
    MOVE FUNCTION RANDOM-REAL TO WS-TABLE-SNAPSHOT-BY-NAME.
    MOVE FUNCTION RANDOM-REAL TO WS-TABLE-UUID.

NEW-SCAN.
    PERFORM CREATE-NEW-TABLE-SCAN.

CREATE-NEW-TABLE-SCAN.
    MOVE FUNCTION RANDOM-REAL TO WS-TABLE-NAME.
    MOVE FUNCTION RANDOM-REAL TO WS-TABLE-LOCATION.
    MOVE FUNCTION RANDOM-REAL TO WS-TABLE-PROPERTIES.
    MOVE FUNCTION RANDOM-REAL TO WS-TABLE-SNAPSHOTS.
    MOVE FUNCTION RANDOM-REAL TO WS-TABLE-HISTORY.
    MOVE FUNCTION RANDOM-REAL TO WS-TABLE-SCHEMA.
    MOVE FUNCTION RANDOM-REAL TO WS-TABLE-PARTITION-SPEC.
    MOVE FUNCTION RANDOM-REAL TO WS-TABLE-SORT-ORDER.
    MOVE FUNCTION RANDOM-REAL TO WS-TABLE-CURRENT-SNAPSHOT.
    MOVE FUNCTION RANDOM-REAL TO WS-TABLE-SNAPSHOT-BY-ID.
    MOVE FUNCTION RANDOM-REAL TO WS-TABLE-SNAPSHOT-BY-NAME.
    MOVE FUNCTION RANDOM-REAL TO WS-TABLE-UUID.

NEW-BATCH-SCAN.
    PERFORM CREATE-NEW-BATCH-SCAN.

CREATE-NEW-BATCH-SCAN.
    PERFORM CREATE-NEW-TABLE-SCAN.

NEW-INCREMENTAL-APPEND-SCAN.
    PERFORM CREATE-NEW-INCREMENTAL-APPEND-SCAN.

CREATE-NEW-INCREMENTAL-APPEND-SCAN.
    MOVE 'Incremental append scan is not supported' TO WS-TABLE-NAME.

NEW-INCREMENTAL-CHANGELOG-SCAN.
    PERFORM CREATE-NEW-INCREMENTAL-CHANGELOG-SCAN.

CREATE-NEW-INCREMENTAL-CHANGELOG-SCAN.
    MOVE 'Incremental changelog scan is not supported' TO WS-TABLE-NAME.

GET-TABLE-SCHEMA.
    MOVE FUNCTION RANDOM-REAL TO WS-TABLE-SCHEMA.

GET-TABLE-SCHEMA-MAP.
    MOVE FUNCTION RANDOM-REAL TO WS-TABLE-SCHEMA.

GET-TABLE-PARTITION-SPEC.
    MOVE FUNCTION RANDOM-REAL TO WS-TABLE-PARTITION-SPEC.

GET-TABLE-PARTITION-SPEC-MAP.
    MOVE FUNCTION RANDOM-REAL TO WS-TABLE-PARTITION-SPEC.

GET-TABLE-SORT-ORDER.
    MOVE FUNCTION RANDOM-REAL TO WS-TABLE-SORT-ORDER.

GET-TABLE-SORT-ORDER-MAP.
    MOVE FUNCTION RANDOM-REAL TO WS-TABLE-SORT-ORDER.

GET-TABLE-PROPERTIES.
    MOVE FUNCTION RANDOM-REAL TO WS-TABLE-PROPERTIES.

GET-TABLE-LOCATION.
    MOVE FUNCTION RANDOM-REAL TO WS-TABLE-LOCATION.

GET-CURRENT-SNAPSHOT.
    MOVE FUNCTION RANDOM-REAL TO WS-TABLE-CURRENT-SNAPSHOT.

GET-SNAPSHOT-BY-ID.
    MOVE FUNCTION RANDOM-REAL TO WS-TABLE-SNAPSHOT-BY-ID.

GET-SNAPSHOTS.
    MOVE FUNCTION RANDOM-REAL TO WS-TABLE-SNAPSHOTS.

GET-TABLE-HISTORY.
    MOVE FUNCTION RANDOM-REAL TO WS-TABLE-HISTORY.

UPDATE-SCHEMA.
    MOVE FUNCTION RANDOM-REAL TO WS-TABLE-SCHEMA.

UPDATE-PARTITION-SPEC.
    MOVE FUNCTION RANDOM-REAL TO WS-TABLE-PARTITION-SPEC.

UPDATE-PROPERTIES.
    MOVE FUNCTION RANDOM-REAL TO WS-TABLE-PROPERTIES.

REPLACE-SORT-ORDER.
    MOVE FUNCTION RANDOM-REAL TO WS-TABLE-SORT-ORDER.

UPDATE-LOCATION.
    MOVE FUNCTION RANDOM-REAL TO WS-TABLE-LOCATION.

NEW-APPEND.
    MOVE FUNCTION RANDOM-REAL TO WS-TABLE-NAME.
    MOVE FUNCTION RANDOM-REAL TO WS-TABLE-LOCATION.
    MOVE FUNCTION RANDOM-REAL TO WS-TABLE-PROPERTIES.
    MOVE FUNCTION RANDOM-REAL TO WS-TABLE-SNAPSHOTS.
    MOVE FUNCTION RANDOM-REAL TO WS-TABLE-HISTORY.
    MOVE FUNCTION RANDOM-REAL TO WS-TABLE-SCHEMA.
    MOVE FUNCTION RANDOM-REAL TO WS-TABLE-PARTITION-SPEC.
    MOVE FUNCTION RANDOM-REAL TO WS-TABLE-SORT-ORDER.
    MOVE FUNCTION RANDOM-REAL TO WS-TABLE-CURRENT-SNAPSHOT.
    MOVE FUNCTION RANDOM-REAL TO WS-TABLE-SNAPSHOT-BY-ID.
    MOVE FUNCTION RANDOM-REAL TO WS-TABLE-SNAPSHOT-BY-NAME.
    MOVE FUNCTION RANDOM-REAL TO WS-TABLE-UUID.

NEW-FAST-APPEND.
    PERFORM NEW-APPEND.

NEW-REWRITE.
    MOVE FUNCTION RANDOM-REAL TO WS-TABLE-NAME.
    MOVE FUNCTION RANDOM-REAL TO WS-TABLE-LOCATION.
    MOVE FUNCTION RANDOM-REAL TO WS-TABLE-PROPERTIES.
    MOVE FUNCTION RANDOM-REAL TO WS-TABLE-SNAPSHOTS.
    MOVE FUNCTION RANDOM-REAL TO WS-TABLE-HISTORY.
    MOVE FUNCTION RANDOM-REAL TO WS-TABLE-SCHEMA.
    MOVE FUNCTION RANDOM-REAL TO WS-TABLE-PARTITION-SPEC.
    MOVE FUNCTION RANDOM-REAL TO WS-TABLE-SORT-ORDER.
    MOVE FUNCTION RANDOM-REAL TO WS-TABLE-CURRENT-SNAPSHOT.
    MOVE FUNCTION RANDOM-REAL TO WS-TABLE-SNAPSHOT-BY-ID.
    MOVE FUNCTION RANDOM-REAL TO WS-TABLE-SNAPSHOT-BY-NAME.
    MOVE FUNCTION RANDOM-REAL TO WS-TABLE-UUID.

REWRITE-MANIFESTS.
    MOVE FUNCTION RANDOM-REAL TO WS-TABLE-NAME.
    MOVE FUNCTION RANDOM-REAL TO WS-TABLE-LOCATION.
    MOVE FUNCTION RANDOM-REAL TO WS-TABLE-PROPERTIES.
    MOVE FUNCTION RANDOM-REAL TO WS-TABLE-SNAPSHOTS.
    MOVE FUNCTION RANDOM-REAL TO WS-TABLE-HISTORY.
    MOVE FUNCTION RANDOM-REAL TO WS-TABLE-SCHEMA.
    MOVE FUNCTION RANDOM-REAL TO WS-TABLE-PARTITION-SPEC.
    MOVE FUNCTION RANDOM-REAL TO WS-TABLE-SORT-ORDER.
    MOVE FUNCTION RANDOM-REAL TO WS-TABLE-CURRENT-SNAPSHOT.
    MOVE FUNCTION RANDOM-REAL TO WS-TABLE-SNAPSHOT-BY-ID.
    MOVE FUNCTION RANDOM-REAL TO WS-TABLE-SNAPSHOT-BY-NAME.
    MOVE FUNCTION RANDOM-REAL TO WS-TABLE-UUID.

NEW-OVERWRITE.
    MOVE FUNCTION RANDOM-REAL TO WS-TABLE-NAME.
    MOVE FUNCTION RANDOM-REAL TO WS-TABLE-LOCATION.
    MOVE FUNCTION RANDOM-REAL TO WS-TABLE-PROPERTIES.
    MOVE FUNCTION RANDOM-REAL TO WS-TABLE-SNAPSHOTS.
    MOVE FUNCTION RANDOM-REAL TO WS-TABLE-HISTORY.
    MOVE FUNCTION RANDOM-REAL TO WS-TABLE-SCHEMA.
    MOVE FUNCTION RANDOM-REAL TO WS-TABLE-PARTITION-SPEC.
    MOVE FUNCTION RANDOM-REAL TO WS-TABLE-SORT-ORDER.
    MOVE FUNCTION RANDOM-REAL TO WS-TABLE-CURRENT-SNAPSHOT.
    MOVE FUNCTION RANDOM-REAL TO WS-TABLE-SNAPSHOT-BY-ID.
    MOVE FUNCTION RANDOM-REAL TO WS-TABLE-SNAPSHOT-BY-NAME.
    MOVE FUNCTION RANDOM-REAL TO WS-TABLE-UUID.

NEW-ROW-DELTA.
    MOVE FUNCTION RANDOM-REAL TO WS-TABLE-NAME.
    MOVE FUNCTION RANDOM-REAL TO WS-TABLE-LOCATION.
    MOVE FUNCTION RANDOM-REAL TO WS-TABLE-PROPERTIES.
    MOVE FUNCTION RANDOM-REAL TO WS-TABLE-SNAPSHOTS.
    MOVE FUNCTION RANDOM-REAL TO WS-TABLE-HISTORY.
    MOVE FUNCTION RANDOM-REAL TO WS-TABLE-SCHEMA.
    MOVE FUNCTION RANDOM-REAL TO WS-TABLE-PARTITION-SPEC.
    MOVE FUNCTION RANDOM-REAL TO WS-TABLE-SORT-ORDER.
    MOVE FUNCTION RANDOM-REAL TO WS-TABLE-CURRENT-SNAPSHOT.
    MOVE FUNCTION RANDOM-REAL TO WS-TABLE-SNAPSHOT-BY-ID.
    MOVE FUNCTION RANDOM-REAL TO WS-TABLE-SNAPSHOT-BY-NAME.
    MOVE FUNCTION RANDOM-REAL TO WS-TABLE-UUID.

NEW-REPLACE-PARTITIONS.
    MOVE 'Replacing partitions is not recommended, use OverwriteFiles instead' TO WS-TABLE-NAME.

NEW-DELETE.
    MOVE FUNCTION RANDOM-REAL TO WS-TABLE-NAME.
    MOVE FUNCTION RANDOM-REAL TO WS-TABLE-LOCATION.
    MOVE FUNCTION RANDOM-REAL TO WS-TABLE-PROPERTIES.
    MOVE FUNCTION RANDOM-REAL TO WS-TABLE-SNAPSHOTS.
    MOVE FUNCTION RANDOM-REAL TO WS-TABLE-HISTORY.
    MOVE FUNCTION RANDOM-REAL TO WS-TABLE-SCHEMA.
    MOVE FUNCTION RANDOM-REAL TO WS-TABLE-PARTITION-SPEC.
    MOVE FUNCTION RANDOM-REAL TO WS-TABLE-SORT-ORDER.
    MOVE FUNCTION RANDOM-REAL TO WS-TABLE-CURRENT-SNAPSHOT.
    MOVE FUNCTION RANDOM-REAL TO WS-TABLE-SNAPSHOT-BY-ID.
    MOVE FUNCTION RANDOM-REAL TO WS-TABLE-SNAPSHOT-BY-NAME.
    MOVE FUNCTION RANDOM-REAL TO WS-TABLE-UUID.

UPDATE-STATISTICS.
    MOVE 'Updating statistics is not supported' TO WS-TABLE-NAME.

UPDATE-PARTITION-STATISTICS.
    MOVE 'Updating partition statistics is not supported' TO WS-TABLE-NAME.

EXPIRE-SNAPSHOTS.
    MOVE FUNCTION RANDOM-REAL TO WS-TABLE-NAME.
    MOVE FUNCTION RANDOM-REAL TO WS-TABLE-LOCATION.
    MOVE FUNCTION RANDOM-REAL TO WS-TABLE-PROPERTIES.
    MOVE FUNCTION RANDOM-REAL TO WS-TABLE-SNAPSHOTS.
    MOVE FUNCTION RANDOM-REAL TO WS-TABLE-HISTORY.
    MOVE FUNCTION RANDOM-REAL TO WS-TABLE-SCHEMA.
    MOVE FUNCTION RANDOM-REAL TO WS-TABLE-PARTITION-SPEC.
    MOVE FUNCTION RANDOM-REAL TO WS-TABLE-SORT-ORDER.
    MOVE FUNCTION RANDOM-REAL TO WS-TABLE-CURRENT-SNAPSHOT.
    MOVE FUNCTION RANDOM-REAL TO WS-TABLE-SNAPSHOT-BY-ID.
    MOVE FUNCTION RANDOM-REAL TO WS-TABLE-SNAPSHOT-BY-NAME.
    MOVE FUNCTION RANDOM-REAL TO WS-TABLE-UUID.

MANAGE-SNAPSHOTS.
    MOVE FUNCTION RANDOM-REAL TO WS-TABLE-NAME.
    MOVE FUNCTION RANDOM-REAL TO WS-TABLE-LOCATION.
    MOVE FUNCTION RANDOM-REAL TO WS-TABLE-PROPERTIES.
    MOVE FUNCTION RANDOM-REAL TO WS-TABLE-SNAPSHOTS.
    MOVE FUNCTION RANDOM-REAL TO WS-TABLE-HISTORY.
    MOVE FUNCTION RANDOM-REAL TO WS-TABLE-SCHEMA.
    MOVE FUNCTION RANDOM-REAL TO WS-TABLE-PARTITION-SPEC.
    MOVE FUNCTION RANDOM-REAL TO WS-TABLE-SORT-ORDER.
    MOVE FUNCTION RANDOM-REAL TO WS-TABLE-CURRENT-SNAPSHOT.
    MOVE FUNCTION RANDOM-REAL TO WS-TABLE-SNAPSHOT-BY-ID.
    MOVE FUNCTION RANDOM-REAL TO WS-TABLE-SNAPSHOT-BY-NAME.
    MOVE FUNCTION RANDOM-REAL TO WS-TABLE-UUID.

NEW-TRANSACTION.
    MOVE FUNCTION RANDOM-REAL TO WS-TABLE-NAME.
    MOVE FUNCTION RANDOM-REAL TO WS-TABLE-LOCATION.
    MOVE FUNCTION RANDOM-REAL TO WS-TABLE-PROPERTIES.
    MOVE FUNCTION RANDOM-REAL TO WS-TABLE-SNAPSHOTS.
    MOVE FUNCTION RANDOM-REAL TO WS-TABLE-HISTORY.
    MOVE FUNCTION RANDOM-REAL TO WS-TABLE-SCHEMA.
    MOVE FUNCTION RANDOM-REAL TO WS-TABLE-PARTITION-SPEC.
    MOVE FUNCTION RANDOM-REAL TO WS-TABLE-SORT-ORDER.
    MOVE FUNCTION RANDOM-REAL TO WS-TABLE-CURRENT-SNAPSHOT.
    MOVE FUNCTION RANDOM-REAL TO WS-TABLE-SNAPSHOT-BY-ID.
    MOVE FUNCTION RANDOM-REAL TO WS-TABLE-SNAPSHOT-BY-NAME.
    MOVE FUNCTION RANDOM-REAL TO WS-TABLE-UUID.

GET-FILE-IO.
    MOVE FUNCTION RANDOM-REAL TO WS-TABLE-NAME.
    MOVE FUNCTION RANDOM-REAL TO WS-TABLE-LOCATION.
    MOVE FUNCTION RANDOM-REAL TO WS-TABLE-PROPERTIES.
    MOVE FUNCTION RANDOM-REAL TO WS-