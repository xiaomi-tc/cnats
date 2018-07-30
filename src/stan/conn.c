// Copyright 2018 The NATS Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include "conn.h"
#include "copts.h"
#include "pub.h"

#include "../sub.h"

// Client send connID in ConnectRequest and PubMsg, and server
// listens and responds to client PINGs. The validity of the
// connection (based on connID) is checked on incoming PINGs.
#define PROTOCOL_ONE    (1)

#ifdef DEV_MODE
// For type safety
void stanConn_Lock(stanConnection *nc)   { natsMutex_Lock(nc->mu);   }
void stanConn_Unlock(stanConnection *nc) { natsMutex_Unlock(nc->mu); }
#endif // DEV_MODE


static void
_freeStanConn(stanConnection *sc)
{
    if (sc == NULL)
        return;

    natsSubscription_Destroy(sc->hbSubscription);
    natsSubscription_Destroy(sc->ackSubscription);
    natsConnection_Destroy(sc->nc);
    natsInbox_Destroy(sc->hbInbox);
    natsStrHash_Destroy(sc->pubAckMap);
    natsCondition_Destroy(sc->pubAckCond);
    natsCondition_Destroy(sc->pubAckMaxInflightCond);
    stanConnOptions_Destroy(sc->opts);
    NATS_FREE(sc->pubMsgBuf);
    NATS_FREE(sc->pubSubjBuf);
    natsMutex_Destroy(sc->pubAckMu);
    natsTimer_Destroy(sc->pubAckTimer);
    natsMutex_Destroy(sc->mu);
    NATS_FREE(sc->clientID);
    NATS_FREE(sc->connID);
    NATS_FREE(sc->pubPrefix);
    NATS_FREE(sc->subRequests);
    NATS_FREE(sc->unsubRequests);
    NATS_FREE(sc->subCloseRequests);
    NATS_FREE(sc->closeRequests);
    NATS_FREE(sc->ackSubject);

    NATS_FREE(sc);

    natsLib_Release();
}

void
stanConn_retain(stanConnection *sc)
{
    if (sc == NULL)
        return;

    stanConn_Lock(sc);
    sc->refs++;
    stanConn_Unlock(sc);
}

void
stanConn_release(stanConnection *sc)
{
    int refs = 0;

    if (sc == NULL)
        return;

    stanConn_Lock(sc);
    refs = --(sc->refs);
    stanConn_Unlock(sc);

    if (refs == 0)
        _freeStanConn(sc);
}


static void
_releaseStanConnCB(void *closure)
{
    stanConnection *sc = (stanConnection*) closure;
    stanConn_release(sc);
}

static void
_stanProcessHeartBeat(natsConnection *nc, natsSubscription *sub, natsMsg *msg, void *closure)
{
    // No payload assumed, just reply.
    natsConnection_Publish(nc, natsMsg_GetReply(msg), NULL, 0);
    natsMsg_Destroy(msg);
}

natsStatus
stanConnection_Connect(stanConnection **newConn, const char* clusterID, const char* clientID, stanConnOptions *opts)
{
    stanConnection      *sc = NULL;
    natsStatus          s;

    if ((newConn == NULL)
            || (clusterID == NULL)
            || (clusterID[0] == '\0')
            || (clientID == NULL)
            || (clientID[0] == '\0'))
    {
        return nats_setDefaultError(NATS_INVALID_ARG);
    }

    s = nats_Open(-1);
    if (s != NATS_OK)
        return s;

    sc = NATS_CALLOC(1, sizeof(stanConnection));
    if (sc == NULL)
        return nats_setDefaultError(NATS_NO_MEMORY);

    s = natsMutex_Create(&sc->mu);
    if (s != NATS_OK)
    {
        NATS_FREE(sc);
        return NATS_UPDATE_ERR_STACK(s);
    }

    natsLib_Retain();

    // Set to 1 so that release free the connection in case of error after this point.
    sc->refs = 1;

    // Set options
    if (opts != NULL)
        s = stanConnOptions_clone(&sc->opts, opts);
    else
        s = stanConnOptions_Create(&sc->opts);

    if ((s == NATS_OK) && (sc->opts->ncOpts == NULL))
        s = natsOptions_Create(&sc->opts->ncOpts);

    // Override NATS connections (but we work on our clone or private one,
    // so that does not affect user's provided options).
    if (s == NATS_OK)
        s = natsOptions_SetName(sc->opts->ncOpts, clientID);
    if (s == NATS_OK)
        s = natsOptions_SetReconnectBufSize(sc->opts->ncOpts, 0);
    if (s == NATS_OK)
        s = natsOptions_SetMaxReconnect(sc->opts->ncOpts, -1);
    if (s == NATS_OK)
        s = natsOptions_SetAllowReconnect(sc->opts->ncOpts, true);

    // Connect to NATS
    if (s == NATS_OK)
        s = natsConnection_Connect(&sc->nc, sc->opts->ncOpts);

    if (s == NATS_OK)
    {
        sc->pubAckMaxInflightThreshold = (int) ((float) sc->opts->maxPubAcksInflight * sc->opts->maxPubAcksInFlightPercentage);
        if (sc->pubAckMaxInflightThreshold <= 0)
            sc->pubAckMaxInflightThreshold = 1;
    }

    // Make a copy of user provided clientID
    IF_OK_DUP_STRING(s, sc->clientID, clientID);

    if (s == NATS_OK)
    {
        char tmpNUID[NUID_BUFFER_LEN + 1];

        s = natsNUID_Next(tmpNUID, NUID_BUFFER_LEN + 1);
        IF_OK_DUP_STRING(s, sc->connID, tmpNUID);
        if (s == NATS_OK)
            sc->connIDLen = (int) strlen(sc->connID);
    }

    // Create maps, etc..
    if (s == NATS_OK)
        s = natsStrHash_Create(&sc->pubAckMap, 16);
    if (s == NATS_OK)
        s = natsCondition_Create(&sc->pubAckCond);
    if (s == NATS_OK)
        s = natsCondition_Create(&sc->pubAckMaxInflightCond);
    if (s == NATS_OK)
        s = natsMutex_Create(&sc->pubAckMu);

    // Create HB inbox and a subscription on that
    if (s == NATS_OK)
        s = natsInbox_Create(&sc->hbInbox);
    if (s == NATS_OK)
    {
        s = natsConnection_Subscribe(&sc->hbSubscription, sc->nc, sc->hbInbox, _stanProcessHeartBeat, NULL);
        if (s == NATS_OK)
        {
            natsSubscription_SetPendingLimits(sc->hbSubscription, -1, -1);
            natsSub_setOnCompleteCB(sc->hbSubscription, _releaseStanConnCB, (void*) sc);
            sc->refs++;
        }
    }

    // Send the connection request
    if (s == NATS_OK)
    {
        Pb__ConnectRequest  connReq;
        int                 reqSize   = 0;
        char                *reqBytes = NULL;
        natsMsg             *replyMsg = NULL;
        char                discoverySubj[256];

        pb__connect_request__init(&connReq);
        connReq.clientid = sc->clientID;
        connReq.connid.data = (uint8_t*) sc->connID;
        connReq.connid.len = sc->connIDLen;
        connReq.heartbeatinbox = sc->hbInbox;
        connReq.protocol = PROTOCOL_ONE;
        connReq.pinginterval = sc->opts->pingInterval;
        connReq.pingmaxout = sc->opts->pingMaxOut;

        reqSize = (int) pb__connect_request__get_packed_size(&connReq);
        if (reqSize == 0)
        {
            s = nats_setError(NATS_ERR, "%s", "connection request protocol packed size is 0");
        }
        else
        {
            reqBytes = NATS_MALLOC(reqSize);
            if (reqBytes == NULL)
                s = nats_setDefaultError(NATS_NO_MEMORY);
        }
        if (s == NATS_OK)
        {
            int packedSize = (int) pb__connect_request__pack(&connReq, (uint8_t*) reqBytes);
            if (reqSize != packedSize)
            {
                s = nats_setError(NATS_ERR, "connect request computed packed size is %d, got %d",
                        reqSize, packedSize);
            }
            else
            {
                snprintf(discoverySubj, sizeof(discoverySubj), "%s.%s", sc->opts->discoveryPrefix, clusterID);
                s = natsConnection_Request(&replyMsg, sc->nc, discoverySubj, reqBytes, reqSize, sc->opts->connTimeout);
                if (s == NATS_TIMEOUT)
                    NATS_UPDATE_ERR_TXT("%s", STAN_ERR_CONNECT_REQUEST_TIMEOUT);
            }
            NATS_FREE(reqBytes);
        }
        if (s == NATS_OK)
        {
            Pb__ConnectResponse *connResp = NULL;

            connResp = pb__connect_response__unpack(NULL,
                    (size_t) natsMsg_GetDataLength(replyMsg),
                    (const uint8_t*) natsMsg_GetData(replyMsg));
            if (connResp == NULL)
                s = nats_setError(NATS_ERR, "%s", "unable to decode connection response");

            if ((s == NATS_OK) && (strlen(connResp->error) > 0))
                s = nats_setError(NATS_ERR, "%s", connResp->error);

            // Duplicate strings
            IF_OK_DUP_STRING(s, sc->pubPrefix, connResp->pubprefix);
            IF_OK_DUP_STRING(s, sc->subRequests, connResp->subrequests);
            IF_OK_DUP_STRING(s, sc->unsubRequests, connResp->unsubrequests);
            IF_OK_DUP_STRING(s, sc->subCloseRequests, connResp->subcloserequests);
            IF_OK_DUP_STRING(s, sc->closeRequests, connResp->closerequests);

            if (s == NATS_OK)
                sc->pubPrefixLen = (int) strlen(sc->pubPrefix);

            pb__connect_response__free_unpacked(connResp, NULL);

            natsMsg_Destroy(replyMsg);
        }
    }
    // Setup (pub) ACK subscription
    if (s == NATS_OK)
    {
        char tmp[11 + NUID_BUFFER_LEN + 1];

        snprintf(tmp, sizeof(tmp), "%s", "_STAN.acks.");
        s = natsNUID_Next(tmp+11, NUID_BUFFER_LEN + 1);
        IF_OK_DUP_STRING(s, sc->ackSubject, (char*)tmp);

        if (s == NATS_OK)
            s = natsConnection_Subscribe(&sc->ackSubscription, sc->nc, sc->ackSubject, stanProcessPubAck, (void*) sc);
        if (s == NATS_OK)
        {
            natsSubscription_SetPendingLimits(sc->ackSubscription, -1, -1);
            natsSub_setOnCompleteCB(sc->ackSubscription, _releaseStanConnCB, (void*) sc);
            sc->refs++;
        }
    }

    if (s == NATS_OK)
        *newConn = sc;
    else
    {
        if (sc->nc != NULL)
            natsConnection_Close(sc->nc);
        stanConn_release(sc);
    }

    return NATS_UPDATE_ERR_STACK(s);
}

static natsStatus
_stanConnClose(stanConnection *sc)
{
    natsStatus          s = NATS_OK;
    Pb__CloseRequest    closeReq;
    int                 reqSize   = 0;
    char                *reqBytes = NULL;
    natsMsg             *replyMsg = NULL;
    _pubAck             *pa       = NULL;
    natsConnection      *nc       = NULL;
    char                *cid      = NULL;
    char                *closeSubj= NULL;
    int64_t             timeout   = 0;

    // Need to release publish call if applicable.

    // Do not grab the connection lock yet since a publish call
    // may be holding the connection lock but wait on the
    // pubAckMaxInflightCond condition variable.
    natsMutex_Lock(sc->pubAckMu);
    if (!sc->pubAckClosed)
    {
        sc->pubAckClosed = true;
        natsCondition_Broadcast(sc->pubAckMaxInflightCond);
    }
    natsMutex_Unlock(sc->pubAckMu);

    stanConn_Lock(sc);
    if (sc->closed)
    {
        stanConn_Unlock(sc);
        return NATS_OK;
    }
    natsMutex_Lock(sc->pubAckMu);
    sc->closed = true;
    // Release possible blocked publish calls
    natsCondition_Broadcast(sc->pubAckCond);
    natsMutex_Unlock(sc->pubAckMu);

    natsSubscription_Unsubscribe(sc->hbSubscription);
    natsSubscription_Unsubscribe(sc->ackSubscription);

    // If there is a timer set, make it trigger soon, this will
    // release the pending pubAcks for async publish calls.
    if (sc->pubAckTimer != NULL)
        natsTimer_Reset(sc->pubAckTimer, 1);

    nc        = sc->nc;
    cid       = sc->clientID;
    closeSubj = sc->closeRequests;
    timeout   = sc->opts->connTimeout;
    stanConn_Unlock(sc);

    pb__close_request__init(&closeReq);
    closeReq.clientid = cid;

    reqSize = (int) pb__close_request__get_packed_size(&closeReq);
    if (reqSize == 0)
    {
        s = nats_setError(NATS_ERR, "%s", "connection close protocol packed size is 0");
    }
    else
    {
        reqBytes = NATS_MALLOC(reqSize);
        if (reqBytes == NULL)
            s = nats_setDefaultError(NATS_NO_MEMORY);
        if (s == NATS_OK)
        {
            int packedSize = (int) pb__close_request__pack(&closeReq, (uint8_t*) reqBytes);
            if (reqSize != packedSize)
            {
                s = nats_setError(NATS_ERR, "connection close request computed packed size is %d, got %v",
                        reqSize, packedSize);
            }
            else
            {
                s = natsConnection_Request(&replyMsg, nc, closeSubj, reqBytes, reqSize, timeout);
                if (s == NATS_TIMEOUT)
                    NATS_UPDATE_ERR_TXT("%s", STAN_ERR_CLOSE_REQUEST_TIMEOUT);
            }

            NATS_FREE(reqBytes);
        }
        if (s == NATS_OK)
        {
            Pb__CloseResponse *closeResp = NULL;

            closeResp = pb__close_response__unpack(NULL,
                    (size_t) natsMsg_GetDataLength(replyMsg),
                    (const uint8_t*) natsMsg_GetData(replyMsg));

            if ((closeResp != NULL) && (strlen(closeResp->error) > 0))
                s = nats_setError(NATS_ERR, "%s", closeResp->error);

            pb__close_response__free_unpacked(closeResp, NULL);
            natsMsg_Destroy(replyMsg);
        }
    }

    natsConnection_Close(nc);

    return NATS_UPDATE_ERR_STACK(s);
}

natsStatus
stanConnection_Close(stanConnection *sc)
{
    natsStatus s;

    if (sc == NULL)
        return NATS_OK;

    s = _stanConnClose(sc);
    return NATS_UPDATE_ERR_STACK(s);
}

natsStatus
stanConnection_Destroy(stanConnection *sc)
{
    natsStatus s;

    if (sc == NULL)
        return NATS_OK;

    s = _stanConnClose(sc);
    stanConn_release(sc);
    return NATS_UPDATE_ERR_STACK(s);
}

natsStatus
expandBuf(char **buf, int *cap, int newcap)
{
    char *newBuf = NULL;

    if (*buf == NULL)
        newBuf = NATS_MALLOC(newcap);
    else
        newBuf = NATS_REALLOC(buf, newcap);
    if (newBuf == NULL)
        return nats_setError(NATS_NO_MEMORY, "unable to expand buffer from %d to %d", *cap, newcap);

    *buf = newBuf; // possibly same if realloc did it in place
    *cap = newcap;

    return NATS_OK;
}
