IDENTIFICATION DIVISION.
PROGRAM-ID. RUNTIMELOGGERMANAGER.

ENVIRONMENT DIVISION.
CONFIGURATION SECTION.
SPECIAL-NAMES.
    CLASS LogLevelConfig IS "org.apache.kafka.common.config.LogLevelConfig"
    CLASS AlterConfigsResource IS "org.apache.kafka.common.message.IncrementalAlterConfigsRequestData.AlterConfigsResource"
    CLASS AlterableConfig IS "org.apache.kafka.common.message.IncrementalAlterConfigsRequestData.AlterableConfig"
    CLASS Errors IS "org.apache.kafka.common.errors.Errors"
    CLASS ClusterAuthorizationException IS "org.apache.kafka.common.errors.ClusterAuthorizationException"
    CLASS InvalidConfigurationException IS "org.apache.kafka.common.errors.InvalidConfigurationException"
    CLASS InvalidRequestException IS "org.apache.kafka.common.errors.InvalidRequestException"
    CLASS OpType IS "org.apache.kafka.clients.admin.AlterConfigOp.OpType"
    CLASS LoggingController IS "kafka.utils.LoggingController"
    CLASS Logger IS "org.slf4j.Logger".

DATA DIVISION.
WORKING-STORAGE SECTION.
01 VALID-LOG-LEVELS-STRING PIC X(100).
01 NODE-ID PIC 9(9) COMP.
01 LOG PIC X(100).

PROCEDURE DIVISION.
MAIN-PROCEDURE.
    MOVE 0 TO NODE-ID.
    MOVE SPACES TO LOG.

    PERFORM APPLY-CHANGES-FOR-RESOURCE.

    STOP RUN.

APPLY-CHANGES-FOR-RESOURCE.
    IF NOT AUTHORIZED-FOR-CLUSTER-RESOURCE
        RAISE ClusterAuthorizationException USING Errors.CLUSTER_AUTHORIZATION_FAILED.MESSAGE()
    END-IF.

    PERFORM VALIDATE-RESOURCE-NAME-IS-NODE-ID.
    PERFORM VALIDATE-LOG-LEVEL-CONFIGS.

    IF NOT VALIDATE-ONLY
        PERFORM ALTER-LOG-LEVEL-CONFIGS.
    END-IF.

VALIDATE-RESOURCE-NAME-IS-NODE-ID.
    MOVE FUNCTION NUMVAL(RESOURCE-NAME) TO REQUEST-ID.
    IF REQUEST-ID NOT = NODE-ID
        RAISE InvalidRequestException USING "Unexpected node id. Expected " NODE-ID ", but received " NODE-ID.
    END-IF.

VALIDATE-LOG-LEVEL-CONFIGS.
    PERFORM VARYING OP IN CONFIGS
        WHEN OPERATION-TYPE = OpType.SET
            PERFORM VALIDATE-LOGGER-NAME-EXISTS USING OP.NAME
            IF NOT LogLevelConfig.VALID_LOG_LEVELS CONTAINS OP.VALUE
                RAISE InvalidConfigurationException USING "Cannot set the log level of " OP.NAME " to " OP.VALUE " as it is not a supported log level. Valid log levels are " VALID-LOG-LEVELS-STRING
            END-IF
        WHEN OPERATION-TYPE = OpType.DELETE
            PERFORM VALIDATE-LOGGER-NAME-EXISTS USING OP.NAME
            IF OP.NAME = LoggingController.ROOT_LOGGER()
                RAISE InvalidRequestException USING "Removing the log level of the " LoggingController.ROOT_LOGGER() " logger is not allowed"
            END-IF
        WHEN OTHER
            RAISE InvalidRequestException USING OPERATION-TYPE " operation is not allowed for the " BROKER_LOGGER " resource"
    END-PERFORM.

VALIDATE-LOGGER-NAME-EXISTS.
    IF NOT LoggingController.LOGGEREXISTS(LOGGER-NAME)
        RAISE InvalidConfigurationException USING "Logger " LOGGER-NAME " does not exist!"
    END-IF.

ALTER-LOG-LEVEL-CONFIGS.
    PERFORM VARYING OP IN CONFIGS
        WHEN OPERATION-TYPE = OpType.SET
            IF LoggingController.LOGLEVEL(OP.NAME, OP.VALUE)
                LOG.WARN "Updated the log level of " OP.NAME " to " OP.VALUE
            ELSE
                LOG.ERROR "Failed to update the log level of " OP.NAME " to " OP.VALUE
            END-IF
        WHEN OPERATION-TYPE = OpType.DELETE
            IF LoggingController.UNSETLOGLEVEL(OP.NAME)
                LOG.WARN "Unset the log level of " OP.NAME
            ELSE
                LOG.ERROR "Failed to unset the log level of " OP.NAME
            END-IF
        WHEN OTHER
            RAISE IllegalArgumentException USING "Invalid log4j configOperation: " OPERATION-TYPE
    END-PERFORM.