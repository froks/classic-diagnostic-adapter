/*
 * SPDX-FileCopyrightText: 2026 Copyright (c) Contributors to the Eclipse Foundation
 *
 * See the NOTICE file(s) distributed with this work for additional
 * information regarding copyright ownership.
 *
 * This program and the accompanying materials are made available under the
 * terms of the Apache License Version 2.0 which is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 *
 * SPDX-License-Identifier: Apache-2.0
 */

use std::{sync::Arc, time::Duration};

use cda_interfaces::dlt_ctx;
use doip_definitions::payload::{AliveCheckResponse, DoipPayload};
use tokio::{
    io::{AsyncRead, AsyncWrite},
    sync::{mpsc, oneshot, watch},
    time::Interval,
};

use crate::{
    DiagnosticResponse,
    connections::{ConnectionResetReason, try_read},
    ecu_connection::{ECUConnectionSend as _, EcuConnectionTarget},
    error::ConnectionError,
};

/// A `DoIP` payload queued for the gateway sender task.
pub(crate) struct OutgoingMessage {
    payload: DoipPayload,
    /// Receives the outcome of writing `payload` to the connection.
    written: oneshot::Sender<Result<(), ConnectionError>>,
    /// Requests a connection reset when the write fails. Set on the last
    /// transmission attempt, when the failure is persistent.
    reset_on_failure: bool,
}

impl OutgoingMessage {
    /// Creates the message and the receiver for its write outcome.
    pub(crate) fn new(
        payload: DoipPayload,
        reset_on_failure: bool,
    ) -> (Self, oneshot::Receiver<Result<(), ConnectionError>>) {
        let (written, written_rx) = oneshot::channel();
        (
            Self {
                payload,
                written,
                reset_on_failure,
            },
            written_rx,
        )
    }

    #[cfg(test)]
    pub(crate) fn payload(&self) -> &DoipPayload {
        &self.payload
    }

    #[cfg(test)]
    pub(crate) fn reset_on_failure(&self) -> bool {
        self.reset_on_failure
    }

    /// Reports the write outcome to the requester, like the sender task does.
    #[cfg(test)]
    pub(crate) fn complete(self, result: Result<(), ConnectionError>) {
        let _ = self.written.send(result);
    }
}

/// Writes `payload` to the gateway connection within `send_timeout`.
async fn write_payload<T>(
    gateway_connection: &EcuConnectionTarget<T>,
    payload: DoipPayload,
    send_timeout: Duration,
) -> Result<(), ConnectionError>
where
    T: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    let mut guard = gateway_connection.lock_send().await?;
    match tokio::time::timeout(send_timeout, guard.get_sender().send(payload)).await {
        Ok(Ok(())) => Ok(()),
        Ok(Err(e)) => {
            tracing::error!(error = ?e, "Failed to send message");
            Err(e)
        }
        Err(_) => {
            tracing::error!(timeout = ?send_timeout, "Timeout sending message");
            Err(ConnectionError::Timeout(format!(
                "Message not sent within {} ms",
                send_timeout.as_millis()
            )))
        }
    }
}

#[tracing::instrument(
    skip_all,
    fields(
        dlt_context = dlt_ctx!("DOIP")
    )
)]
pub(crate) fn spawn_gateway_sender_task<T>(
    gateway_connection: Arc<EcuConnectionTarget<T>>,
    mut inrx: mpsc::Receiver<OutgoingMessage>,
    reset_tx: mpsc::Sender<ConnectionResetReason>,
    send_pending_tx: watch::Sender<bool>,
) -> tokio::task::JoinHandle<()>
where
    T: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    cda_interfaces::spawn_named!(
        &format!(
            "doip-gateway-sender-{}",
            gateway_connection.gateway_doip_config.gateway_ip
        ),
        async move {
            fn send_pending_status(
                send_pending_tx: &watch::Sender<bool>,
                value: bool,
            ) -> Result<(), ()> {
                send_pending_tx.send(value).map_err(|_| {
                    tracing::warn!("Send pending receiver closed");
                })
            }

            let alive_check_interval = gateway_connection
                .gateway_doip_config
                .transport
                .alive_check_interval;
            let send_timeout = gateway_connection
                .gateway_doip_config
                .transport
                .send_timeout;
            let alive_check_enabled = alive_check_interval > Duration::ZERO;
            let mut alive_interval = alive_check_enabled.then(|| {
                let mut interval = tokio::time::interval_at(
                    // If setting a huge value for the alive check this is okay to fail,
                    // as it is guarded by a config sanity check.
                    tokio::time::Instant::now()
                        .checked_add(alive_check_interval)
                        .expect("Failed to set interval, alive check value is too big"),
                    alive_check_interval,
                );
                interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
                interval
            });
            loop {
                tokio::select! {
                    // Per default tokio, randomized which branch is checked first to ensure fairness.
                    // We always want to priotize sending messages over the alive check.
                    // Adding 'biased' means the selects are checked in order from top to bottom.
                    // https://docs.rs/tokio/latest/tokio/macro.select.html#fairness
                    biased;
                    msg = inrx.recv() => {
                        let Some(OutgoingMessage { payload, written, reset_on_failure }) = msg else {
                            // Channel closed - all senders dropped (gateway shut down).
                            tracing::debug!("Send channel closed, shutting down sender task");
                            break;
                        };
                        // let rx task know that we want to send something.
                        if send_pending_status(&send_pending_tx, true).is_err() {
                            break;
                        }

                        let start = std::time::Instant::now();
                        let result = write_payload(&gateway_connection, payload, send_timeout).await;
                        tracing::debug!(
                            total_duration = ?start.elapsed(),
                            success = result.is_ok(),
                            "DoIP send request timing"
                        );

                        let reset_reason = match &result {
                            Err(e) if reset_on_failure => Some(format!("Unable to send message: {e}")),
                            _ => None,
                        };
                        // The requester may have given up waiting; nothing to report then.
                        let _ = written.send(result);

                        // inform the rx task that we are done sending
                        if send_pending_status(&send_pending_tx, false).is_err() {
                            break;
                        }

                        // The last transmission attempt failed as well: the
                        // connection is considered broken.
                        if let Some(reason) = reset_reason
                            && let Err(e) = reset_tx.send(reason).await
                        {
                            tracing::error!(
                                error = ?e,
                                "Failed to send connection reset request"
                            );
                            // Without the reset channel the connection cannot recover.
                            break;
                        }
                        // Reset the alive check timer so it only fires after a full
                        // interval of silence - never during active communication.
                       alive_interval.as_mut().map(Interval::reset);
                    },

                   _ = async {
                        match alive_interval.as_mut() {
                                Some(interval) => interval.tick().await,
                                None => std::future::pending().await,
                            }
                        }, if alive_check_enabled => {
                        if send_pending_status(&send_pending_tx, true).is_err() {
                            break;
                        }

                        let (alive_response, conn_gateway_name, conn_gateway_ip) = {
                            (
                                send_alive_response(&gateway_connection).await,
                                gateway_connection.gateway_doip_config.name.clone(),
                                gateway_connection.gateway_doip_config.gateway_ip.clone(),
                            )
                        };

                        if let Err(e) = alive_response {
                            tracing::error!(
                                error = ?e,
                                gateway_name = %conn_gateway_name,
                                gateway_ip = %conn_gateway_ip,
                                "Failed to send alive check request, resetting connection"
                            );
                            // no need for any 'sleep' here, the reset task holds the connection
                            // lock until the connection is ready again.
                            if let Err(e) = reset_tx
                                .send("Unable to send alive check request".to_owned())
                                .await
                            {
                                tracing::error!(
                                    error = ?e,
                                    "Failed to send connection reset request"
                                );
                                // if the reset channel is closed, we cannot reset the connection
                                // and there is no point in continuing
                                break;
                            }
                        }

                        if send_pending_status(&send_pending_tx, false).is_err() {
                            break;
                        }
                    }
                }
            }
        }
    )
}

async fn send_alive_response<T>(conn: &EcuConnectionTarget<T>) -> Result<(), ()>
where
    T: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    async fn handle_alive_request_response<T>(conn: &EcuConnectionTarget<T>)
    where
        T: AsyncRead + AsyncWrite + Unpin + Send + 'static,
    {
        if tokio::time::timeout(Duration::from_secs(1), async {
            let Ok(mut reader_mtx) = conn.lock_read().await else {
                return;
            };
            let reader = reader_mtx.get_reader();
            match try_read(Duration::from_secs(1), reader).await {
                Some(Ok(DiagnosticResponse::AliveCheckResponse)) => {
                    tracing::debug!("Alive check OK");
                }
                Some(Ok(DiagnosticResponse::Ack(_))) => {
                    tracing::debug!("Received ACK");
                    Box::pin(handle_alive_request_response(conn)).await;
                }
                Some(Ok(DiagnosticResponse::GenericNack(_))) => {
                    tracing::debug!("Received Generic NACK");
                }
                Some(Ok(msg)) => {
                    tracing::error!(msg = ?msg, "Received Unrelated msg");
                }
                Some(Err(e)) => {
                    tracing::error!(error = %e, "Error reading alive check response");
                }
                None => {
                    tracing::debug!("Timeout waiting for alive check response");
                }
            }
        })
        .await
        .is_err()
        {
            tracing::debug!("Timeout waiting for alive check response");
        }
    }

    // TODO: handle alive request errors according to spec, see #430
    let Ok(mut sender) = conn.lock_send().await else {
        return Err(());
    };
    match sender
        .get_sender()
        .send(DoipPayload::AliveCheckResponse(AliveCheckResponse {
            source_address: conn.gateway_doip_config.tester_address,
        }))
        .await
    {
        Ok(()) => {
            handle_alive_request_response(conn).await;
            Ok(())
        }
        Err(e) => {
            tracing::error!(error = ?e, "Failed to send alive check request");
            Err(())
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{sync::Arc, time::Duration};

    use doip_definitions::{
        header::ProtocolVersion,
        payload::{DoipPayload, EntityStatusRequest},
    };
    use tokio::sync::{Mutex, mpsc, watch};

    use crate::{
        ConnectionError, DoipTransportConfig, GatewayDoipConfig,
        connection_sender::{OutgoingMessage, spawn_gateway_sender_task},
        connections::ConnectionResetReason,
        ecu_connection::{EcuConnectionReadVariant, EcuConnectionSendVariant, EcuConnectionTarget},
        socket::{DoIPConnection, DoipSocketConfig},
    };

    /// Builds a duplex-backed `EcuConnectionTarget` together with the server-side
    /// `DoIPConnection` used to respond to alive checks and dummy messages.
    /// `lock_send()` succeeds and `alive_interval.reset()` is called on each
    /// message processed by the sender task.
    fn duplex_ecu_connection_target(
        alive_check_interval: Duration,
    ) -> (
        EcuConnectionTarget<tokio::io::DuplexStream>,
        DoIPConnection<tokio::io::DuplexStream>,
    ) {
        let (client, server) = tokio::io::duplex(1024);
        let config = DoipSocketConfig {
            protocol_version: ProtocolVersion::Iso13400_2012,
            send_diagnostic_message_ack: false,
        };
        let server_conn = DoIPConnection::new(server, config);
        let (read_half, write_half) = DoIPConnection::new(client, config).into_split();
        let target = EcuConnectionTarget {
            ecu_connection_rx: Mutex::new(Some(EcuConnectionReadVariant::Plain(read_half))),
            ecu_connection_tx: Mutex::new(Some(EcuConnectionSendVariant::Plain(write_half))),
            gateway_doip_config: GatewayDoipConfig {
                gateway_ip: "127.0.0.1".to_owned(),
                name: String::new(),
                tester_address: [0, 0],
                protocol_version: ProtocolVersion::Iso13400_2010,
                transport: DoipTransportConfig {
                    tester_ip: "127.0.0.1".to_owned(),
                    port: 13400,
                    tls_port: 0,
                    send_diagnostic_message_ack: false,
                    send_timeout: Duration::from_secs(5),
                    alive_check_interval,
                },
            },
        };
        (target, server_conn)
    }

    /// Shared test harness for gateway sender task tests.
    /// Creates all channels, the duplex connection, pauses time, spawns the task,
    /// and starts a background responder that simulates a real gateway on the
    /// server side of the duplex.
    ///
    /// The responder handles two types of incoming messages:
    /// - `AliveCheckRequest`: replies with `AliveCheckResponse` so the sender's
    ///   alive check completes immediately instead of timing out.
    /// - `EntityStatusRequest` (dummy diagnostic messages sent by the test):
    ///   replies with `DiagnosticMessageAck` to simulate a real gateway that
    ///   acknowledges incoming diagnostic messages. Without this, the ack would
    ///   be missing from the read buffer, which would make the test less realistic.
    ///
    /// The test then uses the returned handles to drive the scenario.
    struct GatewaySenderTestHarness {
        msg_tx: mpsc::Sender<OutgoingMessage>,
        reset_rx: mpsc::Receiver<ConnectionResetReason>,
        _gateway_conn: Arc<EcuConnectionTarget<tokio::io::DuplexStream>>,
        _send_pending_rx: watch::Receiver<bool>,
        task: tokio::task::JoinHandle<()>,
    }

    impl GatewaySenderTestHarness {
        fn new(alive_check_interval: Duration) -> Self {
            tokio::time::pause();

            let (msg_tx, msg_rx) = mpsc::channel::<OutgoingMessage>(10);
            let (reset_tx, reset_rx) = mpsc::channel::<ConnectionResetReason>(10);
            let (send_pending_tx, send_pending_rx) = watch::channel(false);
            let (gateway_target, _) = duplex_ecu_connection_target(alive_check_interval);

            let gateway_conn = Arc::new(gateway_target);

            let task = spawn_gateway_sender_task(
                Arc::clone(&gateway_conn),
                msg_rx,
                reset_tx,
                send_pending_tx,
            );

            Self {
                msg_tx,
                reset_rx,
                _gateway_conn: gateway_conn,
                _send_pending_rx: send_pending_rx,
                task,
            }
        }

        /// Sends a dummy diagnostic message through the message channel.
        ///
        /// The payload (`EntityStatusRequest`) is just a placeholder - the only
        /// thing that matters is that sending any message through `msg_tx` triggers
        /// `alive_interval.reset()` inside the sender task.  The exact variant is
        /// irrelevant for the timer behavior we are testing here.
        ///
        /// After sending, we yield once so the sender task is polled and actually
        /// processes the message.
        async fn send_dummy_message(&self) {
            let (msg, _written) = OutgoingMessage::new(
                DoipPayload::EntityStatusRequest(EntityStatusRequest {}),
                false,
            );
            self.msg_tx.send(msg).await.unwrap();
            tokio::task::yield_now().await;
        }
    }

    /// This test validates that the alive check timer resets after each message send,
    /// ensuring the alive check only fires after a full interval of idle time.
    #[tokio::test]
    async fn alive_check_only_fires_when_idle() {
        let harness = GatewaySenderTestHarness::new(Duration::from_secs(10));

        // Sending messages before the interval elapses prevents alive check.
        // Send a message at t=4s (before 10s interval).
        tokio::time::advance(Duration::from_secs(4)).await;
        harness.send_dummy_message().await;

        // Advance another 4s (total 8s from last send, still within 10s interval).
        tokio::time::advance(Duration::from_secs(4)).await;
        harness.send_dummy_message().await;

        // Advance 9s (total 9s from last send, still within 10s interval).
        tokio::time::advance(Duration::from_secs(9)).await;
        tokio::task::yield_now().await;

        // The alive check interval has not elapsed since the last message send -
        // the sender task should still be running and no reset should have been requested.
        assert!(
            !harness.task.is_finished(),
            "Alive check should not have fired during active communication"
        );

        // Advance 2 more seconds (total 11s from last send, exceeds 10s interval).
        // The alive check fires: the responder task automatically replies with
        // AliveCheckResponse, so the request/response cycle completes without
        // waiting for any timeout.
        tokio::time::advance(Duration::from_secs(2)).await;
        // Yield twice so the sender task sends the request, the responder replies,
        // and the sender reads the response.
        tokio::task::yield_now().await;
        tokio::task::yield_now().await;

        // Task should still be alive after the alive check fired.
        assert!(
            !harness.task.is_finished(),
            "Task should still be running after alive check fired"
        );

        // After alive check fires, sending a message resets the timer.
        harness.send_dummy_message().await;

        // Advance 9s (within 10s interval from last send).
        tokio::time::advance(Duration::from_secs(9)).await;
        tokio::task::yield_now().await;

        assert!(
            !harness.task.is_finished(),
            "Alive check should not fire after timer reset by message send"
        );

        // Advance 2 more seconds (total 11s from last send) - alive check fires again.
        tokio::time::advance(Duration::from_secs(2)).await;
        tokio::task::yield_now().await;
        tokio::task::yield_now().await;

        assert!(
            !harness.task.is_finished(),
            "Task should still be running after second alive check fired"
        );

        // Clean up: abort the task.
        harness.task.abort();
        let _ = harness.task.await;
    }

    /// Validates that the alive check does not fire when the interval is set to zero
    /// (disabled).
    #[tokio::test]
    async fn alive_check_disabled_when_interval_zero() {
        let mut harness = GatewaySenderTestHarness::new(Duration::ZERO);

        // Advance a very long time - alive check should never fire.
        tokio::time::advance(Duration::from_secs(4200)).await;
        tokio::task::yield_now().await;

        assert!(
            harness.reset_rx.try_recv().is_err(),
            "Alive check should never fire when interval is zero (disabled)"
        );
        assert!(!harness.task.is_finished(), "Task should still be running");

        // Clean up: abort the task.
        harness.task.abort();
        let _ = harness.task.await;
    }

    /// Validates that when both a message and the alive check are ready simultaneously,
    /// the biased select ensures the message branch wins.
    #[tokio::test]
    async fn biased_select_prioritizes_messages_over_alive_check() {
        let mut harness = GatewaySenderTestHarness::new(Duration::from_secs(5));

        // Advance time past the alive check interval so both tick and message are ready.
        tokio::time::advance(Duration::from_secs(6)).await;
        // Send a message - both the tick and the message channel are now ready.
        harness.send_dummy_message().await;

        // Due to biased select the message branch is evaluated before the alive check
        // branch. The message is processed first, which resets the timer - so the
        // alive check does not fire this iteration and no reset is triggered.
        assert!(
            harness.reset_rx.try_recv().is_err(),
            "Biased select should process the message before the alive check,              \
             preventing an immediate reset"
        );
        assert!(!harness.task.is_finished(), "Task should still be running");

        // Clean up: abort the task.
        harness.task.abort();
        let _ = harness.task.await;
    }

    /// Spawns the sender task on a duplex connection without alive checks.
    /// Dropping the returned server side makes every write fail.
    fn spawn_sender() -> (
        mpsc::Sender<OutgoingMessage>,
        mpsc::Receiver<ConnectionResetReason>,
        watch::Receiver<bool>,
        crate::socket::DoIPConnection<tokio::io::DuplexStream>,
    ) {
        let (msg_tx, msg_rx) = mpsc::channel::<OutgoingMessage>(10);
        let (reset_tx, reset_rx) = mpsc::channel::<ConnectionResetReason>(10);
        let (send_pending_tx, send_pending_rx) = watch::channel(false);
        let (gateway_target, server) = duplex_ecu_connection_target(Duration::ZERO);
        spawn_gateway_sender_task(Arc::new(gateway_target), msg_rx, reset_tx, send_pending_tx);
        (msg_tx, reset_rx, send_pending_rx, server)
    }

    fn entity_status_request() -> DoipPayload {
        DoipPayload::EntityStatusRequest(EntityStatusRequest {})
    }

    /// A successful write is reported to the requester.
    #[tokio::test]
    async fn sender_reports_successful_write() {
        let (msg_tx, _reset_rx, _send_pending_rx, _server) = spawn_sender();

        let (msg, written) = OutgoingMessage::new(entity_status_request(), true);
        msg_tx.send(msg).await.expect("Failed to queue message");

        let result = tokio::time::timeout(Duration::from_secs(1), written)
            .await
            .expect("timed out waiting for write outcome")
            .expect("sender task dropped the message");
        assert!(result.is_ok(), "expected Ok, got {result:?}");
    }

    /// A failed write is reported to the requester. Without `reset_on_failure`
    /// the connection is left alone, so the request can be repeated.
    #[tokio::test]
    async fn sender_reports_failed_write_without_reset() {
        let (msg_tx, mut reset_rx, mut send_pending_rx, server) = spawn_sender();
        drop(server);

        let (msg, written) = OutgoingMessage::new(entity_status_request(), false);
        msg_tx.send(msg).await.expect("Failed to queue message");

        let result = tokio::time::timeout(Duration::from_secs(1), written)
            .await
            .expect("timed out waiting for write outcome")
            .expect("sender task dropped the message");
        assert!(
            matches!(result, Err(ConnectionError::SendFailed(_))),
            "expected SendFailed, got {result:?}"
        );

        // The task is done with the message once it cleared the send-pending flag.
        tokio::time::timeout(
            Duration::from_secs(1),
            send_pending_rx.wait_for(|pending| !pending),
        )
        .await
        .expect("send pending flag not cleared")
        .expect("sender task stopped");
        assert!(
            reset_rx.try_recv().is_err(),
            "no reset expected before the last attempt"
        );
    }

    /// When the last transmission attempt fails, the sender task requests a
    /// connection reset after reporting the failure.
    #[tokio::test]
    async fn sender_requests_reset_when_last_attempt_fails() {
        let (msg_tx, mut reset_rx, _send_pending_rx, server) = spawn_sender();
        drop(server);

        let (msg, written) = OutgoingMessage::new(entity_status_request(), true);
        msg_tx.send(msg).await.expect("Failed to queue message");

        let result = tokio::time::timeout(Duration::from_secs(1), written)
            .await
            .expect("timed out waiting for write outcome")
            .expect("sender task dropped the message");
        assert!(result.is_err(), "expected an error, got {result:?}");

        let reason = tokio::time::timeout(Duration::from_secs(1), reset_rx.recv())
            .await
            .expect("timed out waiting for reset request")
            .expect("reset channel closed");
        assert!(
            reason.contains("Unable to send message"),
            "unexpected reset reason: {reason}"
        );
    }
}
