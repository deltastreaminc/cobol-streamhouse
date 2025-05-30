IDENTIFICATION DIVISION.
PROGRAM-ID. METADATACACHE.

ENVIRONMENT DIVISION.
CONFIGURATION SECTION.
REPOSITORY.
    COPY "KAFKABROKERREGISTRATION.cpy".
    COPY "PARTITIONREGISTRATION.cpy".
    COPY "KAFKAFEATURES.cpy".
    COPY "KAFKAMETADATAVERSION.cpy".

DATA DIVISION.
WORKING-STORAGE SECTION.
01 WS-TOPICS-MAP.
    05 WS-TOPICS-TO-IDS    PIC X(36) OCCURS 1000 TIMES.
    05 WS-IDS-TO-TOPICS    PIC X(128) OCCURS 1000 TIMES.
01 WS-BROKER-TO-NODES.
    05 WS-BROKER-ID        PIC 9(9) OCCURS 1000 TIMES.
    05 WS-BROKER-NODES     PIC X(256) OCCURS 1000 TIMES.
01 WS-PARTITION-INFOS.
    05 WS-TOPIC-NAME       PIC X(128).
    05 WS-PARTITION-ID     PIC 9(9).
    05 WS-LEADER-NODE      PIC X(256).
    05 WS-REPLICAS         PIC 9(9) OCCURS 100 TIMES.
    05 WS-ISR             PIC 9(9) OCCURS 100 TIMES.
    05 WS-OFFLINE-REPLICAS PIC 9(9) OCCURS 100 TIMES.
01 WS-INTERNAL-TOPICS.
    05 WS-INTERNAL-TOPIC   PIC X(128) OCCURS 100 TIMES.
01 WS-CONTROLLER-NODE     PIC X(256).
01 WS-CLUSTER-ID          PIC X(36).
01 WS-TOPIC-METADATA.
    05 WS-TOPIC-NAME       PIC X(128).
    05 WS-PARTITION-DATA.
        10 WS-PARTITION-ID PIC 9(9).
        10 WS-LEADER-NODE  PIC X(256).
        10 WS-REPLICAS     PIC 9(9) OCCURS 100 TIMES.
        10 WS-ISR          PIC 9(9) OCCURS 100 TIMES.
        10 WS-OFFLINE-REPLICAS PIC 9(9) OCCURS 100 TIMES.
01 WS-BROKER-METADATA.
    05 WS-BROKER-ID        PIC 9(9).
    05 WS-BROKER-RACK      PIC X(32).

PROCEDURE DIVISION.

    PERFORM KRAFT-METADATA-CACHE.

KRAFT-METADATA-CACHE SECTION.
    MOVE BROKERID TO WS-BROKER-ID.
    MOVE KRAFTVERSIONSUPPLIER TO WS-KRAFTVERSION-SUPPLIER.
    PERFORM BUILD-CLUSTER.
    PERFORM GET-TOPIC-METADATA.
    EXIT SECTION.

BUILD-CLUSTER SECTION.
    PERFORM INITIALIZE-BROKER-TO-NODES.
    PERFORM INITIALIZE-PARTITION-INFOS.
    PERFORM INITIALIZE-INTERNAL-TOPICS.
    PERFORM GET-CONTROLLER-NODE.
    MOVE WS-CLUSTER-ID TO CLUSTER-ID.
    MOVE WS-BROKER-TO-NODES TO BROKERTONODES.
    MOVE WS-PARTITION-INFOS TO PARTITIONINFOS.
    MOVE WS-INTERNAL-TOPICS TO INTERNALTOPICS.
    MOVE WS-CONTROLLER-NODE TO CONTROLLERNODE.
    PERFORM CONSTRUCT-CLUSTER.
    EXIT SECTION.

INITIALIZE-BROKER-TO-NODES SECTION.
    MOVE 0 TO WS-BROKER-ID-INDEX.
    PERFORM VARYING I FROM 1 BY 1 UNTIL I > 1000
        MOVE I TO WS-BROKER-ID(WS-BROKER-ID-INDEX)
        MOVE SPACES TO WS-BROKER-NODES(WS-BROKER-ID-INDEX)
        ADD 1 TO WS-BROKER-ID-INDEX
    END-PERFORM.
    EXIT SECTION.

INITIALIZE-PARTITION-INFOS SECTION.
    MOVE SPACES TO WS-TOPIC-NAME.
    MOVE 0 TO WS-PARTITION-ID.
    MOVE SPACES TO WS-LEADER-NODE.
    MOVE 0 TO WS-REPLICAS(1), WS-ISR(1), WS-OFFLINE-REPLICAS(1).
    EXIT SECTION.

INITIALIZE-INTERNAL-TOPICS SECTION.
    MOVE SPACES TO WS-INTERNAL-TOPIC(1).
    EXIT SECTION.

GET-CONTROLLER-NODE SECTION.
    PERFORM GET-RANDOM-ALIVE-BROKER.
    IF WS-BROKER-ID(WS-RANDOM-BROKER-INDEX) = -1
        MOVE NODE-NO-NODE TO WS-CONTROLLER-NODE
    ELSE
        MOVE WS-BROKER-NODES(WS-RANDOM-BROKER-INDEX) TO WS-CONTROLLER-NODE
    END-IF.
    EXIT SECTION.

GET-RANDOM-ALIVE-BROKER SECTION.
    PERFORM VARYING I FROM 1 BY 1 UNTIL I > 1000 OR WS-BROKER-ID(I) = -1
        CONTINUE
    END-PERFORM.
    IF I > 1000
        MOVE -1 TO WS-RANDOM-BROKER-INDEX
    ELSE
        MOVE I TO WS-RANDOM-BROKER-INDEX
    END-IF.
    EXIT SECTION.

CONSTRUCT-CLUSTER SECTION.
    MOVE WS-BROKER-TO-NODES TO CLUSTER-BROKERS.
    MOVE WS-PARTITION-INFOS TO CLUSTER-PARTITIONS.
    MOVE WS-INTERNAL-TOPICS TO CLUSTER-INTERNAL-TOPICS.
    MOVE WS-CONTROLLER-NODE TO CLUSTER-CONTROLLER.
    PERFORM CREATE-CLUSTER.
    EXIT SECTION.

CREATE-CLUSTER SECTION.
    MOVE WS-CLUSTER-ID TO CLUSTER-ID.
    PERFORM VARYING I FROM 1 BY 1 UNTIL I > 1000
        IF WS-BROKER-ID(I) NOT = -1
            MOVE WS-BROKER-NODES(I) TO CLUSTERNODE
            PERFORM ADD-CLUSTER-NODE
        END-IF
    END-PERFORM.
    EXIT SECTION.

ADD-CLUSTER-NODE SECTION.
    PERFORM VARYING J FROM 1 BY 1 UNTIL J > 100
        IF WS-REPLICAS(J) NOT = 0
            MOVE WS-REPLICAS(J) TO REPLICAID
            PERFORM GET-BROKER-NODES
            PERFORM ADD-PARTITION-INFO
        END-IF
    END-PERFORM.
    EXIT SECTION.

GET-BROKER-NODES SECTION.
    MOVE WS-REPLICAS(J) TO WS-BROKER-ID-INDEX.
    MOVE WS-BROKER-NODES(WS-BROKER-ID-INDEX) TO REPLICANODES.
    EXIT SECTION.

ADD-PARTITION-INFO SECTION.
    MOVE WS-TOPIC-NAME TO TOPICNAME.
    MOVE WS-PARTITION-ID TO PARTITIONID. 
    MOVE WS-LEADER-NODE TO LEADERNODE.
    MOVE WS-REPLICAS TO REPLICAS.
    MOVE WS-ISR TO ISR.
    MOVE WS-OFFLINE-REPLICAS TO OFFLINEREPLICAIDS.
    PERFORM CREATE-PARTITION-INFO.
    EXIT SECTION.

CREATE-PARTITION-INFO SECTION.
    PERFORM VARYING K FROM 1 BY 1 UNTIL K > 100
        IF WS-OFFLINE-REPLICAS(K) NOT = 0
            MOVE WS-OFFLINE-REPLICAS(K) TO OFFLINEREPLICAID
            PERFORM GET-BROKER-NODES
            PERFORM ADD-OFFLINE-REPLICA
        END-IF
    END-PERFORM.
    EXIT SECTION.

ADD-OFFLINE-REPLICA SECTION.
    MOVE REPLICANODES TO OFFLINEREPLICANODES.
    PERFORM ADD-PARTITION-TO-CLUSTER.
    EXIT SECTION.

ADD-PARTITION-TO-CLUSTER SECTION.
    MOVE TOPICNAME TO PARTITIONINFO-TOPICNAME.
    MOVE PARTITIONID TO PARTITIONINFO-PARTITIONID.
    MOVE LEADERNODE TO PARTITIONINFO-LEADERNODE.
    MOVE REPLICAS TO PARTITIONINFO-REPLICAS.
    MOVE ISR TO PARTITIONINFO-ISR.
    MOVE OFFLINEREPLICANODES TO PARTITIONINFO-OFFLINEREPLICANODES.
    PERFORM CREATE-PARTITION-INFO-OBJECT.
    EXIT SECTION.

CREATE-PARTITION-INFO-OBJECT SECTION.
    PERFORM CREATE-PARTITIONINFO.
    MOVE PARTITIONINFO TO WS-PARTITION-INFOS.
    EXIT SECTION.

GET-TOPIC-METADATA SECTION.
    PERFORM VARYING I FROM 1 BY 1 UNTIL I > 1000
        IF WS-TOPICS-TO-IDS(I) NOT = SPACES
            MOVE WS-TOPICS-TO-IDS(I) TO TOPICID
            MOVE WS-IDS-TO-TOPICS(I) TO TOPICNAME
            PERFORM GET-TOPIC-PARTITIONS
            PERFORM ADD-TOPIC-METADATA
        END-IF
    END-PERFORM.
    EXIT SECTION.

GET-TOPIC-PARTITIONS SECTION.
    MOVE TOPICNAME TO WS-TOPIC-NAME.
    MOVE 0 TO WS-PARTITION-ID.
    MOVE SPACES TO WS-LEADER-NODE.
    MOVE 0 TO WS-REPLICAS(1), WS-ISR(1), WS-OFFLINE-REPLICAS(1).
    PERFORM VARYING J FROM 1 BY 1 UNTIL J > 100
        IF WS-REPLICAS(J) NOT = 0
            MOVE J TO WS-PARTITION-ID
            PERFORM GET-PARTITION-LEADER
            PERFORM GET-PARTITION-REPLICAS
            PERFORM GET-PARTITION-ISR
            PERFORM GET-PARTITION-OFFLINE-REPLICAS
            PERFORM ADD-PARTITION-DATA
        END-IF
    END-PERFORM.
    EXIT SECTION.

GET-PARTITION-LEADER SECTION.
    MOVE WS-REPLICAS(J) TO WS-BROKER-ID-INDEX.
    MOVE WS-BROKER-NODES(WS-BROKER-ID-INDEX) TO WS-LEADER-NODE.
    EXIT SECTION.

GET-PARTITION-REPLICAS SECTION.
    MOVE 1 TO K.
    PERFORM VARYING L FROM 1 BY 1 UNTIL L > 100
        IF WS-REPLICAS(L) NOT = 0
            MOVE WS-REPLICAS(L) TO WS-REPLICAS(K)
            ADD 1 TO K
        END-IF
    END-PERFORM.
    EXIT SECTION.

GET-PARTITION-ISR SECTION.
    MOVE 1 TO K.
    PERFORM VARYING L FROM 1 BY 1 UNTIL L > 100
        IF WS-ISR(L) NOT = 0
            MOVE WS-ISR(L) TO WS-ISR(K)
            ADD 1 TO K
        END-IF
    END-PERFORM.
    EXIT SECTION.

GET-PARTITION-OFFLINE-REPLICAS SECTION.
    MOVE 1 TO K.
    PERFORM VARYING L FROM 1 BY 1 UNTIL L > 100
        IF WS-OFFLINE-REPLICAS(L) NOT = 0
            MOVE WS-OFFLINE-REPLICAS(L) TO WS-OFFLINE-REPLICAS(K)
            ADD 1 TO K
        END-IF
    END-PERFORM.
    EXIT SECTION.

ADD-PARTITION-DATA SECTION.
    MOVE WS-TOPIC-NAME TO WS-TOPIC-METADATA-TOPIC-NAME.
    MOVE WS-PARTITION-ID TO WS-TOPIC-METADATA-PARTITION-ID.
    MOVE WS-LEADER-NODE TO WS-TOPIC-METADATA-LEADER-NODE.
    MOVE WS-REPLICAS TO WS-TOPIC-METADATA-REPLICAS.
    MOVE WS-ISR TO WS-TOPIC-METADATA-ISR.
    MOVE WS-OFFLINE-REPLICAS TO WS-TOPIC-METADATA-OFFLINE-REPLICAS.
    PERFORM ADD-TOPIC-METADATA-PARTITION.
    EXIT SECTION.

ADD-TOPIC-METADATA SECTION.
    MOVE WS-TOPIC-METADATA-TOPIC-NAME TO TOPICNAME.
    MOVE WS-TOPIC-METADATA-PARTITION-DATA TO TOPICPARTITIONS.
    PERFORM CREATE-TOPIC-METADATA-OBJECT.
    EXIT SECTION.

ADD-TOPIC-METADATA-PARTITION SECTION.
    MOVE WS-TOPIC-METADATA-PARTITION-ID TO PARTITIONID.
    MOVE WS-TOPIC-METADATA-LEADER-NODE TO LEADERNODE.
    MOVE WS-TOPIC-METADATA-REPLICAS TO REPLICAS.
    MOVE WS-TOPIC-METADATA-ISR TO ISR.
    MOVE WS-TOPIC-METADATA-OFFLINE-REPLICAS TO OFFLINEREPLICAIDS.
    PERFORM CREATE-PARTITION-METADATA-OBJECT.
    EXIT SECTION.

CREATE-TOPIC-METADATA-OBJECT SECTION.
    MOVE TOPICNAME TO TOPICMETADATA-TOPICNAME.
    MOVE TOPICPARTITIONS TO TOPICMETADATA-PARTITIONS.
    PERFORM CREATE-TOPICMETADATA-OBJECT.
    EXIT SECTION.

CREATE-PARTITION-METADATA-OBJECT SECTION.
    MOVE PARTITIONID TO PARTITIONMETADATA-PARTITIONID.
    MOVE LEADERNODE TO PARTITIONMETADATA-LEADERNODE.
    MOVE REPLICAS TO PARTITIONMETADATA-REPLICAS.
    MOVE ISR TO PARTITIONMETADATA-ISR.
    MOVE OFFLINEREPLICAIDS TO PARTITIONMETADATA-OFFLINEREPLICAIDS.
    PERFORM CREATE-PARTITIONMETADATA-OBJECT.
    EXIT SECTION.

STOP RUN.