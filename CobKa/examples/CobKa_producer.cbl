IDENTIFICATION DIVISION.
       PROGRAM-ID. KAFKA-PRODUCER.
       AUTHOR. Shawn Gordon.
       
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
               
           01 KAFKA-PRODUCER             PIC X(8).
           01 KAFKA-MESSAGE.
               05 MESSAGE-VALUE          PIC X(1000).
               05 MESSAGE-KEY            PIC X(100).
               05 PARTITION-VALUE        PIC 9(5) VALUE 0.
               
           01 KAFKA-DELIVERY-REPORT.
               05 DELIVERY-STATUS        PIC X(10).
               05 DELIVERY-TOPIC         PIC X(100).
               05 DELIVERY-PARTITION     PIC 9(5).
               05 DELIVERY-OFFSET        PIC 9(10).
               05 DELIVERY-ERROR         PIC X(100).
               
           01 USER-INPUT                 PIC X(1000).
           01 EOF-FLAG                   PIC X VALUE 'N'.
           01 PRODUCER-STATUS            PIC X(20).
           01 FLUSH-TIMEOUT              PIC 9(5) VALUE 15000.
           
       PROCEDURE DIVISION.
       MAIN-PROCEDURE.
           DISPLAY "KAFKA PRODUCER PROGRAM".
           
           PERFORM GET-KAFKA-CONFIG.
           PERFORM INITIALIZE-PRODUCER.
           PERFORM PROCESS-MESSAGES UNTIL EOF-FLAG = 'Y'.
           PERFORM CLOSE-PRODUCER.
           
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
           
       INITIALIZE-PRODUCER.
           DISPLAY "Attempting to create Kafka producer connection...".
           
           CALL "KAFKA-CREATE-PRODUCER" USING 
               BOOTSTRAP-SERVER
               SECURITY-PROTOCOL
               SASL-MECHANISM
               USERNAME
               PASSWORD
               KAFKA-PRODUCER
               PRODUCER-STATUS.
               
           IF PRODUCER-STATUS NOT = "SUCCESS"
               DISPLAY "Failed to create producer: " PRODUCER-STATUS
               STOP RUN
           ELSE
               DISPLAY "Kafka producer successfully created".
               
           CALL "KAFKA-INIT-DELIVERY-HANDLER" USING KAFKA-PRODUCER.
           
       PROCESS-MESSAGES.
           DISPLAY "Enter messages to send to the topic. Empty line to exit.".
           DISPLAY "Enter message: ".
           ACCEPT USER-INPUT.
           
           IF USER-INPUT = SPACES
               MOVE 'Y' TO EOF-FLAG
               DISPLAY "Empty message received. Exiting."
           ELSE
               MOVE USER-INPUT TO MESSAGE-VALUE
               MOVE TOPIC-NAME TO DELIVERY-TOPIC
               MOVE ZEROS TO PARTITION-VALUE
               
               CALL "KAFKA-PRODUCE-MESSAGE" USING
                   KAFKA-PRODUCER
                   TOPIC-NAME
                   MESSAGE-VALUE
                   PARTITION-VALUE
                   PRODUCER-STATUS
                   
               IF PRODUCER-STATUS NOT = "SUCCESS"
                   DISPLAY "Failed to produce message: " PRODUCER-STATUS
               END-IF
           END-IF.
           
       CLOSE-PRODUCER.
           DISPLAY "Flushing messages...".
           
           CALL "KAFKA-FLUSH-PRODUCER" USING
               KAFKA-PRODUCER
               FLUSH-TIMEOUT
               PRODUCER-STATUS.
               
           DISPLAY "All pending messages delivered. Goodbye!".
           
           CALL "KAFKA-CLOSE-PRODUCER" USING
               KAFKA-PRODUCER.
               
       DELIVERY-REPORT SECTION.
           ENTRY "DELIVERY-HANDLER" USING KAFKA-DELIVERY-REPORT.
               
           IF DELIVERY-ERROR NOT = SPACES
               DISPLAY "Delivery failed: " DELIVERY-ERROR
           ELSE
               DISPLAY "Message delivered to topic " DELIVERY-TOPIC
                   " [" DELIVERY-PARTITION "] at offset " DELIVERY-OFFSET
           END-IF.
           
           EXIT PROGRAM.
