IDENTIFICATION DIVISION.
       PROGRAM-ID. KAFKA-CONSUMER.
       AUTHOR. COBOL-CONVERTER.
       
       ENVIRONMENT DIVISION.
       CONFIGURATION SECTION.
       INPUT-OUTPUT SECTION.
       FILE-CONTROL.
       
       DATA DIVISION.
       FILE SECTION.
       WORKING-STORAGE SECTION.
           01 KAFKA-CONFIG.
               05 BOOTSTRAP-SERVER       PIC X(100).
               05 SECURITY-PROTOCOL      PIC X(20).
               05 SASL-MECHANISM         PIC X(20).
               05 USERNAME               PIC X(50).
               05 PASSWORD               PIC X(50).
               05 TOPIC-NAME             PIC X(100).
               05 GROUP-ID               PIC X(50).
               05 AUTO-OFFSET-RESET      PIC X(10) VALUE 'EARLIEST'.
               05 ENABLE-AUTO-COMMIT     PIC X(5) VALUE 'TRUE'.
               
           01 KAFKA-CONSUMER             PIC X(8).
           01 KAFKA-MESSAGE.
               05 MESSAGE-VALUE          PIC X(1000).
               05 MESSAGE-KEY            PIC X(100).
               05 MESSAGE-TOPIC          PIC X(100).
               05 MESSAGE-PARTITION      PIC 9(5).
               05 MESSAGE-OFFSET         PIC 9(10).
               
           01 CONSUMER-STATUS            PIC X(20).
           01 POLL-TIMEOUT               PIC 9(5) VALUE 100.
           01 RUN-FLAG                   PIC X VALUE 'Y'.
           01 SIGNAL-VALUE               PIC X(10).
           01 EVENT-TYPE                 PIC X(20).
           01 ERROR-CODE                 PIC 9(5).
           01 ERROR-TEXT                 PIC X(100).
           
       PROCEDURE DIVISION.
       MAIN-PROCEDURE.
           DISPLAY "KAFKA CONSUMER PROGRAM".
           
           PERFORM GET-KAFKA-CONFIG.
           PERFORM INITIALIZE-CONSUMER.
           PERFORM PROCESS-MESSAGES UNTIL RUN-FLAG = 'N'.
           PERFORM CLOSE-CONSUMER.
           
           STOP RUN.
           
       GET-KAFKA-CONFIG.
           DISPLAY "Enter bootstrap server (e.g., localhost:9092): ".
           ACCEPT BOOTSTRAP-SERVER.
           
           DISPLAY "Enter security protocol (e.g., SASL_SSL): ".
           ACCEPT SECURITY-PROTOCOL.
           
           DISPLAY "Enter SASL mechanism (e.g., PLAIN): ".
           ACCEPT SASL-MECHANISM.
           
           DISPLAY "Enter username: ".
           ACCEPT USERNAME.
           
           DISPLAY "Enter password: ".
           ACCEPT PASSWORD.
           
           DISPLAY "Enter Kafka topic name: ".
           ACCEPT TOPIC-NAME.
           
           DISPLAY "Enter consumer group ID: ".
           ACCEPT GROUP-ID.
           
       INITIALIZE-CONSUMER.
           DISPLAY "Attempting to create Kafka consumer connection...".
           
           CALL "KAFKA-CREATE-CONSUMER" USING 
               BOOTSTRAP-SERVER
               SECURITY-PROTOCOL
               SASL-MECHANISM
               USERNAME
               PASSWORD
               GROUP-ID
               AUTO-OFFSET-RESET
               ENABLE-AUTO-COMMIT
               KAFKA-CONSUMER
               CONSUMER-STATUS.
               
           IF CONSUMER-STATUS NOT = "SUCCESS"
               DISPLAY "Failed to create consumer: " CONSUMER-STATUS
               STOP RUN
           ELSE
               DISPLAY "Kafka consumer successfully created".
               
           CALL "KAFKA-SUBSCRIBE-TOPIC" USING
               KAFKA-CONSUMER
               TOPIC-NAME
               CONSUMER-STATUS.
               
           IF CONSUMER-STATUS NOT = "SUCCESS"
               DISPLAY "Failed to subscribe to topic: " CONSUMER-STATUS
               STOP RUN
           ELSE
               DISPLAY "Successfully subscribed to topic: " TOPIC-NAME.
               
           DISPLAY "Reading messages from the beginning of the topic...".
           DISPLAY "Press Ctrl+C to exit".
           
           CALL "INIT-SIGNAL-HANDLER".
           
       PROCESS-MESSAGES.
           CALL "CHECK-SIGNAL" USING SIGNAL-VALUE.
           
           IF SIGNAL-VALUE NOT = SPACES
               DISPLAY "Caught signal " SIGNAL-VALUE ": terminating"
               MOVE 'N' TO RUN-FLAG
           ELSE
               CALL "KAFKA-POLL-MESSAGE" USING
                   KAFKA-CONSUMER
                   POLL-TIMEOUT
                   EVENT-TYPE
                   KAFKA-MESSAGE
                   ERROR-CODE
                   ERROR-TEXT
                   
               EVALUATE EVENT-TYPE
                   WHEN "MESSAGE"
                       DISPLAY "Received message from topic " MESSAGE-TOPIC
                               " [" MESSAGE-PARTITION "] at offset " MESSAGE-OFFSET
                       DISPLAY "Key: " MESSAGE-KEY
                       DISPLAY "Value: " MESSAGE-VALUE
                       DISPLAY "-----------------------------------"
                   WHEN "ERROR"
                       DISPLAY "Error: " ERROR-TEXT
                       
                       IF ERROR-CODE = 104 *> Equivalent to kafka.ErrAllBrokersDown
                           MOVE 'N' TO RUN-FLAG
                       END-IF
                   WHEN OTHER
                       CONTINUE
               END-EVALUATE
           END-IF.
           
       CLOSE-CONSUMER.
           DISPLAY "Consumer shutting down...".
           
           CALL "KAFKA-CLOSE-CONSUMER" USING
               KAFKA-CONSUMER.
       
       SIGNAL-HANDLER SECTION.
           ENTRY "INIT-SIGNAL-HANDLER".
               *> This would set up OS-specific signal handling
               *> Implementation would depend on the COBOL runtime environment
               EXIT PROGRAM.
               
           ENTRY "CHECK-SIGNAL" USING SIGNAL-VALUE.
               *> This would check if a signal has been received
               *> and return it in SIGNAL-VALUE if so
               EXIT PROGRAM.