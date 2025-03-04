IDENTIFICATION DIVISION.
PROGRAM-ID. OAUTH2UTIL.

ENVIRONMENT DIVISION.
REPOSITORY.
    COPY "HTTPHeaders.cpy".
    COPY "HTTPRequest.cpy".
    COPY "ImmutableHTTPRequest.cpy".
    COPY "RESTClient.cpy".
    COPY "RESTUtil.cpy".
    COPY "ResourcePaths.cpy".
    COPY "OAuthTokenResponse.cpy".
    COPY "JsonUtil.cpy".
    COPY "Pair.cpy".
    COPY "Tasks.cpy".
    COPY "OAuth2Properties.cpy".

DATA DIVISION.
WORKING-STORAGE SECTION.
01 VALID-SCOPE-TOKEN PIC X(256) VALUE '^[!-~&&[^"\\]]+$'.
01 SCOPE-DELIMITER PIC X(1) VALUE ' '.
01 SCOPE-JOINER PIC X(1) VALUE ' '.
01 AUTHORIZATION-HEADER PIC X(16) VALUE 'Authorization'.
01 BEARER-PREFIX PIC X(7) VALUE 'Bearer '.
01 BASIC-PREFIX PIC X(7) VALUE 'Basic '.
01 CREDENTIAL-SPLITTER PIC X(1) VALUE ':'.
01 GRANT-TYPE PIC X(10) VALUE 'grant_type'.
01 CLIENT-CREDENTIALS PIC X(14) VALUE 'client_credentials'.
01 TOKEN-EXCHANGE PIC X(32) VALUE 'urn:ietf:params:oauth:grant-type:token-exchange'.
01 SCOPE PIC X(5) VALUE 'scope'.
01 CLIENT-ID PIC X(9) VALUE 'client_id'.
01 CLIENT-SECRET PIC X(12) VALUE 'client_secret'.
01 SUBJECT-TOKEN PIC X(13) VALUE 'subject_token'.
01 SUBJECT-TOKEN-TYPE PIC X(16) VALUE 'subject_token_type'.
01 ACTOR-TOKEN PIC X(10) VALUE 'actor_token'.
01 ACTOR-TOKEN-TYPE PIC X(14) VALUE 'actor_token_type'.
01 ACCESS-TOKEN PIC X(13) VALUE 'access_token'.
01 TOKEN-TYPE PIC X(9) VALUE 'token_type'.
01 EXPIRES-IN PIC X(8) VALUE 'expires_in'.
01 ISSUED-TOKEN-TYPE PIC X(16) VALUE 'issued_token_type'.

PROCEDURE DIVISION.

authHeaders.
    IF token NOT EQUAL SPACES THEN
        RETURN FUNCTION JSONBUILD('Authorization', FUNCTION CONCATENATE(BEARER-PREFIX, token))
    ELSE
        RETURN FUNCTION JSONBUILD().

basicAuthHeaders.
    IF credential NOT EQUAL SPACES THEN
        RETURN FUNCTION JSONBUILD('Authorization', FUNCTION CONCATENATE(BASIC-PREFIX, FUNCTION Base64Encode(credential)))
    ELSE
        RETURN FUNCTION JSONBUILD().

isValidScopeToken.
    EVALUATE TRUE
        WHEN FUNCTION MATCHES(scopeToken, VALID-SCOPE-TOKEN)
            RETURN TRUE
        WHEN OTHER
            RETURN FALSE
    END-EVALUATE.

parseScope.
    RETURN FUNCTION SPLIT(scope, SCOPE-DELIMITER).

toScope.
    RETURN FUNCTION CONCATENATE(scopes, SCOPE-JOINER).

buildOptionalParam.
    MOVE FUNCTION JSONBUILD(OAuth2Properties-SCOPE, FUNCTION COALESCE(properties(OAuth2Properties-SCOPE), OAuth2Properties-CATALOG-SCOPE)) TO optionalParamBuilder.
    PERFORM VARYING key IN OAuth2Properties-AUDIENCE, OAuth2Properties-RESOURCE
        IF properties(key) NOT EQUAL SPACES THEN
            MOVE FUNCTION JSONBUILD(key, properties(key)) TO optionalParamBuilder
        END-IF
    END-PERFORM.
    RETURN FUNCTION JSONBUILDBUILD(optionalParamBuilder).

refreshToken.
    MOVE FUNCTION tokenExchangeRequest(subjectToken, subjectTokenType, scope, oauth2ServerUri, optionalOAuthParams) TO request.
    MOVE FUNCTION postForm(client, oauth2ServerUri, request, OAuthTokenResponse, headers, ErrorHandlers-oauthErrorHandler) TO response.
    CALL response-validate.
    RETURN response.

exchangeToken.
    MOVE FUNCTION tokenExchangeRequest(subjectToken, subjectTokenType, actorToken, actorTokenType, scope, optionalParams) TO request.
    MOVE FUNCTION postForm(client, oauth2ServerUri, request, OAuthTokenResponse, headers, ErrorHandlers-oauthErrorHandler) TO response.
    CALL response-validate.
    RETURN response.

fetchToken.
    MOVE FUNCTION clientCredentialsRequest(credential, scope, optionalOAuthParams) TO request.
    MOVE FUNCTION postForm(client, oauth2ServerUri, request, OAuthTokenResponse, headers, ErrorHandlers-oauthErrorHandler) TO response.
    CALL response-validate.
    RETURN response.

tokenResponseToJson.
    CALL JsonUtil-generate(response, tokenResponseToJson-sub).

tokenResponseToJson-sub.
    WRITE JSONSTART.
    WRITE JSONSTRINGFIELD(ACCESS-TOKEN, response-token).
    WRITE JSONSTRINGFIELD(TOKEN-TYPE, response-tokenType).
    IF response-issuedTokenType NOT EQUAL SPACES THEN
        WRITE JSONSTRINGFIELD(ISSUED-TOKEN-TYPE, response-issuedTokenType)
    END-IF.
    IF response-expiresInSeconds NOT EQUAL SPACES THEN
        WRITE JSONNUMBERFIELD(EXPIRES-IN, response-expiresInSeconds)
    END-IF.
    IF response-scopes NOT EQUAL SPACES THEN
        WRITE JSONSTRINGFIELD(SCOPE, toScope(response-scopes))
    END-IF.
    WRITE JSONEND.

tokenResponseFromJson.
    CALL JsonUtil-parse(json, tokenResponseFromJson-sub).

tokenResponseFromJson-sub.
    EVALUATE TRUE
        WHEN FUNCTION ISOBJECT(json)
            MOVE FUNCTION JSONGETSTRING(ACCESS-TOKEN, json) TO builder-token
            MOVE FUNCTION JSONGETSTRING(TOKEN-TYPE, json) TO builder-tokenType
            MOVE FUNCTION JSONGETSTRING(ISSUED-TOKEN-TYPE, json) TO builder-issuedTokenType
            IF FUNCTION HASKEY(EXPIRES-IN, json) THEN
                MOVE FUNCTION JSONGETINT(EXPIRES-IN, json) TO builder-expirationInSeconds
            END-IF
            IF FUNCTION HASKEY(SCOPE, json) THEN
                MOVE FUNCTION JSONGETSTRING(SCOPE, json) TO scope
                CALL parseScope(scope) GIVING builder-scopes
            END-IF
            RETURN FUNCTION OAuthTokenResponse-builder-build
        WHEN OTHER
            RETURN NULL
    END-EVALUATE.

expiresAtMillis.
    IF token NOT EQUAL SPACES THEN
        MOVE FUNCTION SPLIT(token, '.') TO parts
        IF FUNCTION LENGTH(parts) NOT EQUAL 3 THEN
            RETURN NULL
        END-IF
        MOVE FUNCTION JsonUtil-readTree(FUNCTION Base64Decode(parts(2))) TO node
        MOVE FUNCTION JsonUtil-getLongOrNull('exp', node) TO expiresAtSeconds
        IF expiresAtSeconds NOT EQUAL SPACES THEN
            RETURN FUNCTION MULTIPLY(expiresAtSeconds, 1000)
        END-IF
    END-IF.
    RETURN NULL.

STOP RUN.