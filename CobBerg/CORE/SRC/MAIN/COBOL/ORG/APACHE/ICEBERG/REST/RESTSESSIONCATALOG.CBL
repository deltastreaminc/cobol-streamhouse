IDENTIFICATION DIVISION.
PROGRAM-ID. REST-SESSION-CATALOG.

ENVIRONMENT DIVISION.

DATA DIVISION.
WORKING-STORAGE SECTION.
   01 DEFAULT-FILE-IO-IMPL PIC X(50) VALUE "org.apache.iceberg.io.ResolvingFileIO".
   01 REST-METRICS-REPORTING-ENABLED PIC X(30) VALUE "rest-metrics-reporting-enabled".
   01 REST-SNAPSHOT-LOADING-MODE PIC X(30) VALUE "snapshot-loading-mode".
   01 VIEW-ENDPOINTS-SUPPORTED PIC X(30) VALUE "view-endpoints-supported".
   01 REST-PAGE-SIZE PIC X(30) VALUE "rest-page-size".
   01 TOKEN-PREFERENCE-ORDER PIC X(300) VALUE "id-token-type,access-token-type,jwt-token-type,saml2-token-type,saml1-token-type".
   01 TABLE-SESSION-ALLOW-LIST PIC X(100) VALUE "token,id-token-type,access-token-type,jwt-token-type,saml2-token-type,saml1-token-type".
   01 DEFAULT-ENDPOINTS PIC X(1000) VALUE "v1-list-namespaces,v1-load-namespace,v1-create-namespace,v1-update-namespace,v1-delete-namespace,v1-list-tables,v1-load-table,v1-create-table,v1-update-table,v1-delete-table,v1-rename-table,v1-register-table,v1-report-metrics,v1-commit-transaction".
   01 VIEW-ENDPOINTS PIC X(300) VALUE "v1-list-views,v1-load-view,v1-create-view,v1-update-view,v1-delete-view,v1-rename-view".

PROCEDURE DIVISION.

   IDENTIFICATION DIVISION.
   PROGRAM-ID. REST-SESSION-CATALOG.

   ENVIRONMENT DIVISION.

   DATA DIVISION.
   WORKING-STORAGE SECTION.
      01 DEFAULT-FILE-IO-IMPL PIC X(50) VALUE "org.apache.iceberg.io.ResolvingFileIO".
      01 REST-METRICS-REPORTING-ENABLED PIC X(30) VALUE "rest-metrics-reporting-enabled".
      01 REST-SNAPSHOT-LOADING-MODE PIC X(30) VALUE "snapshot-loading-mode".
      01 VIEW-ENDPOINTS-SUPPORTED PIC X(30) VALUE "view-endpoints-supported".
      01 REST-PAGE-SIZE PIC X(30) VALUE "rest-page-size".
      01 TOKEN-PREFERENCE-ORDER PIC X(300) VALUE "id-token-type,access-token-type,jwt-token-type,saml2-token-type,saml1-token-type".
      01 TABLE-SESSION-ALLOW-LIST PIC X(100) VALUE "token,id-token-type,access-token-type,jwt-token-type,saml2-token-type,saml1-token-type".
      01 DEFAULT-ENDPOINTS PIC X(1000) VALUE "v1-list-namespaces,v1-load-namespace,v1-create-namespace,v1-update-namespace,v1-delete-namespace,v1-list-tables,v1-load-table,v1-create-table,v1-update-table,v1-delete-table,v1-rename-table,v1-register-table,v1-report-metrics,v1-commit-transaction".
      01 VIEW-ENDPOINTS PIC X(300) VALUE "v1-list-views,v1-load-view,v1-create-view,v1-update-view,v1-delete-view,v1-rename-view".

   PROCEDURE DIVISION.
       PERFORM INITIALIZE-REST-SESSION-CATALOG.
       PERFORM CLOSE-REST-SESSION-CATALOG.
       STOP RUN.

   INITIALIZE-REST-SESSION-CATALOG.
       CALL "INITIALIZE" USING NAME, UNRESOLVED.
   END-INITIALIZE-REST-SESSION-CATALOG.

   CLOSE-REST-SESSION-CATALOG.
       CALL "CLOSE".
   END-CLOSE-REST-SESSION-CATALOG.

END PROGRAM REST-SESSION-CATALOG.