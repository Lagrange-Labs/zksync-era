use std::{collections::HashMap, sync::Arc};

use crate::metrics::CIRCUIT_PROVER_METRICS;
use crate::types::circuit_prover_payload::GpuCircuitProverPayload;
use anyhow::Context;
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use shivini::ProverContext;
use std::time::Instant;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use zksync_object_store::ObjectStore;
use zksync_prover_dal::ProverDal;
use zksync_prover_dal::{ConnectionPool, Prover};
use zksync_prover_fri_types::FriProofWrapper;
use zksync_prover_fri_types::{
    circuit_definitions::boojum::cs::implementations::setup::FinalizationHintsForProver,
    get_current_pod_name, ProverServiceDataKey,
};
use zksync_prover_job_processor::Executor;
use zksync_prover_job_processor::JobPicker;
use zksync_prover_job_processor::JobSaver;
use zksync_prover_job_processor::{Backoff, BackoffAndCancellable, JobRunner};
use zksync_prover_keystore::GoldilocksGpuProverSetupData;
use zksync_types::{protocol_version::ProtocolSemanticVersion, prover_dal::FriProverJobMetadata};

use crate::{
    gpu_circuit_prover::{
        GpuCircuitProverExecutor, GpuCircuitProverJobPicker, GpuCircuitProverJobSaver,
    },
    types::witness_vector_generator_execution_output::WitnessVectorGeneratorExecutionOutput,
    witness_vector_generator::{
        HeavyWitnessVectorMetadataLoader, LightWitnessVectorMetadataLoader,
        WitnessVectorGeneratorExecutor, WitnessVectorGeneratorJobPicker,
        WitnessVectorGeneratorJobSaver, WitnessVectorMetadataLoader,
    },
};

/// Convenience struct helping with building Witness Vector Generator runners.
#[derive(Debug)]
pub struct WvgRunnerBuilder {
    connection_pool: ConnectionPool<Prover>,
    object_store: Arc<dyn ObjectStore>,
    protocol_version: ProtocolSemanticVersion,
    finalization_hints_cache: HashMap<ProverServiceDataKey, Arc<FinalizationHintsForProver>>,
    sender: mpsc::Sender<(WitnessVectorGeneratorExecutionOutput, FriProverJobMetadata)>,
    cancellation_token: CancellationToken,
    pod_name: String,
}

impl WvgRunnerBuilder {
    pub fn new(
        connection_pool: ConnectionPool<Prover>,
        object_store: Arc<dyn ObjectStore>,
        protocol_version: ProtocolSemanticVersion,
        finalization_hints_cache: HashMap<ProverServiceDataKey, Arc<FinalizationHintsForProver>>,
        sender: mpsc::Sender<(WitnessVectorGeneratorExecutionOutput, FriProverJobMetadata)>,
        cancellation_token: CancellationToken,
    ) -> Self {
        Self {
            connection_pool,
            object_store,
            protocol_version,
            finalization_hints_cache,
            sender,
            cancellation_token,
            pod_name: get_current_pod_name(),
        }
    }

    /// Witness Vector Generator runner implementation for light jobs.
    pub fn light_wvg_runner(
        &self,
        count: usize,
    ) -> JobRunner<
        WitnessVectorGeneratorExecutor,
        WitnessVectorGeneratorJobPicker<LightWitnessVectorMetadataLoader>,
        WitnessVectorGeneratorJobSaver,
    > {
        let metadata_loader =
            LightWitnessVectorMetadataLoader::new(self.pod_name.clone(), self.protocol_version);

        self.wvg_runner(count, metadata_loader)
    }

    /// Witness Vector Generator runner implementation that prioritizes heavy jobs over light jobs.
    pub fn heavy_wvg_runner(
        &self,
        count: usize,
    ) -> JobRunner<
        WitnessVectorGeneratorExecutor,
        WitnessVectorGeneratorJobPicker<HeavyWitnessVectorMetadataLoader>,
        WitnessVectorGeneratorJobSaver,
    > {
        let metadata_loader =
            HeavyWitnessVectorMetadataLoader::new(self.pod_name.clone(), self.protocol_version);

        self.wvg_runner(count, metadata_loader)
    }

    /// Creates a Witness Vector Generator job runner with specified MetadataLoader.
    /// The MetadataLoader makes the difference between heavy & light WVG runner.
    fn wvg_runner<ML: WitnessVectorMetadataLoader>(
        &self,
        count: usize,
        metadata_loader: ML,
    ) -> JobRunner<
        WitnessVectorGeneratorExecutor,
        WitnessVectorGeneratorJobPicker<ML>,
        WitnessVectorGeneratorJobSaver,
    > {
        let executor = WitnessVectorGeneratorExecutor;
        let job_picker = WitnessVectorGeneratorJobPicker::new(
            self.connection_pool.clone(),
            self.object_store.clone(),
            self.finalization_hints_cache.clone(),
            metadata_loader,
        );
        let job_saver =
            WitnessVectorGeneratorJobSaver::new(self.connection_pool.clone(), self.sender.clone());
        let backoff = Backoff::default();

        JobRunner::new(
            executor,
            job_picker,
            job_saver,
            count,
            Some(BackoffAndCancellable::new(
                backoff,
                self.cancellation_token.clone(),
            )),
        )
    }
}

/// Circuit Prover runner implementation.
pub fn circuit_prover_runner(
    connection_pool: ConnectionPool<Prover>,
    object_store: Arc<dyn ObjectStore>,
    protocol_version: ProtocolSemanticVersion,
    setup_data_cache: HashMap<ProverServiceDataKey, Arc<GoldilocksGpuProverSetupData>>,
    receiver: mpsc::Receiver<(WitnessVectorGeneratorExecutionOutput, FriProverJobMetadata)>,
    prover_context: ProverContext,
) -> JobRunner<GpuCircuitProverExecutor, GpuCircuitProverJobPicker, GpuCircuitProverJobSaver> {
    let executor = GpuCircuitProverExecutor::new(prover_context);
    let job_picker = GpuCircuitProverJobPicker::new(receiver, setup_data_cache);
    let job_saver = GpuCircuitProverJobSaver::new(connection_pool, object_store, protocol_version);
    JobRunner::new(executor, job_picker, job_saver, 1, None)
}

pub struct ProxyExecutor {
    lpn_gateway_connection_tx: mpsc::Sender<lagrange_grpc::SubmitTaskRequest>,
    lpn_gateway_connection_rx: mpsc::Receiver<lagrange_grpc::SubmitTaskResponse>,
}

impl ProxyExecutor {
    pub fn new(
        lpn_gateway_connection_tx: mpsc::Sender<lagrange_grpc::SubmitTaskRequest>,
        lpn_gateway_connection_rx: mpsc::Receiver<lagrange_grpc::SubmitTaskResponse>,
    ) -> Self {
        Self {
            lpn_gateway_connection_tx,
            lpn_gateway_connection_rx,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Deserialize, Serialize)]
pub struct MessageEnvelope<T> {
    pub query_id: String,
    pub task_id: String,
    pub db_task_id: Option<i32>,
    pub rtt: u64,
    pub gas: Option<u64>,
    pub routing_key: RoutingKey,

    pub inner: T,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub enum TaskType {
    Flat(Vec<u8>),
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
pub struct RoutingKey {
    domain: String,
    priority: u64,
}
impl RoutingKey {
    fn default() -> Self {
        RoutingKey {
            domain: "sp".into(),
            priority: 2,
        }
    }
}

#[derive(serde::Serialize, serde::Deserialize)]
pub enum LPNWork {
    CircuitProverTask {
        input: GpuCircuitProverPayload,
        metadata: FriProverJobMetadata,
    },
}

impl Executor for ProxyExecutor {
    type Input = GpuCircuitProverPayload;
    type Output = FriProofWrapper;
    type Metadata = FriProverJobMetadata;

    fn execute(
        &self,
        input: Self::Input,
        metadata: Self::Metadata,
    ) -> anyhow::Result<Self::Output> {
        let work = LPNWork::CircuitProverTask { input, metadata };

        let task = MessageEnvelope::<TaskType> {
            query_id: String::new(),
            task_id: format!(
                "id: {}, block: {}, circuit: {}, round: {}",
                metadata.id, metadata.block_number, metadata.circuit_id, metadata.aggregation_round
            ),
            db_task_id: Some(5),
            rtt: 13,
            gas: None,
            routing_key: RoutingKey::default(),
            inner: TaskType::Flat(Vec::new()),
        };
        let serialized = serde_json::to_string(&task)?;
        tracing::info!("Sending Task: {}B", serialized.len());

        let envelope = lagrange_grpc::SubmitTaskRequest {
            request: Some(lagrange_grpc::submit_task_request::Request::Task(
                serialized,
            )),
        };

        self.lpn_gateway_connection_tx
            .blocking_send(envelope)
            .unwrap();

        anyhow::bail!("bis später");
    }
}

#[derive(Debug)]
pub struct ProxyCircuitProverJobPicker {
    receiver: mpsc::Receiver<(WitnessVectorGeneratorExecutionOutput, FriProverJobMetadata)>,
    setup_data_cache: HashMap<ProverServiceDataKey, Arc<GoldilocksGpuProverSetupData>>,
}

impl ProxyCircuitProverJobPicker {
    pub fn new(
        receiver: mpsc::Receiver<(WitnessVectorGeneratorExecutionOutput, FriProverJobMetadata)>,
        setup_data_cache: HashMap<ProverServiceDataKey, Arc<GoldilocksGpuProverSetupData>>,
    ) -> Self {
        Self {
            receiver,
            setup_data_cache,
        }
    }
}

#[async_trait]
impl JobPicker for ProxyCircuitProverJobPicker {
    type ExecutorType = ProxyExecutor;

    async fn pick_job(
        &mut self,
    ) -> anyhow::Result<Option<(GpuCircuitProverPayload, FriProverJobMetadata)>> {
        let start_time = Instant::now();
        tracing::info!("Started picking gpu circuit prover job");

        let (wvg_output, metadata) = self
            .receiver
            .recv()
            .await
            .context("no witness vector generators are available, stopping...")?;
        let WitnessVectorGeneratorExecutionOutput {
            circuit,
            witness_vector,
        } = wvg_output;

        let key = ProverServiceDataKey {
            circuit_id: metadata.circuit_id,
            round: metadata.aggregation_round,
        }
        .crypto_setup_key();
        let setup_data = self
            .setup_data_cache
            .get(&key)
            .context("failed to retrieve setup data from cache")?
            .clone();

        let payload = GpuCircuitProverPayload {
            circuit,
            witness_vector,
            setup_data,
        };
        tracing::info!(
            "Finished picking gpu circuit prover job {}, on batch {}, for circuit {}, at round {} in {:?}",

            metadata.id,
            metadata.block_number,
            metadata.circuit_id,
            metadata.aggregation_round,
            start_time.elapsed()
        );
        CIRCUIT_PROVER_METRICS
            .load_time
            .observe(start_time.elapsed());
        Ok(Some((payload, metadata)))
    }
}
#[derive(Debug)]
pub struct ProxyCircuitProverJobSaver {
    connection_pool: ConnectionPool<Prover>,
    object_store: Arc<dyn ObjectStore>,
    protocol_version: ProtocolSemanticVersion,
}

impl ProxyCircuitProverJobSaver {
    pub fn new(
        connection_pool: ConnectionPool<Prover>,
        object_store: Arc<dyn ObjectStore>,
        protocol_version: ProtocolSemanticVersion,
    ) -> Self {
        Self {
            connection_pool,
            object_store,
            protocol_version,
        }
    }
}

pub mod lagrange_grpc {
    tonic::include_proto!("lagrange");
}

#[async_trait]
impl JobSaver for ProxyCircuitProverJobSaver {
    type ExecutorType = ProxyExecutor;

    #[tracing::instrument(
        name = "gpu_circuit_prover_job_saver",
        skip_all,
        fields(l1_batch = % data.1.block_number)
    )]
    async fn save_job_result(
        &self,
        data: (anyhow::Result<FriProofWrapper>, FriProverJobMetadata),
    ) -> anyhow::Result<()> {
        let start_time = Instant::now();
        let (result, metadata) = data;
        tracing::info!(
            "Started saving gpu circuit prover job {}, on batch {}, for circuit {}, at round {}",
            metadata.id,
            metadata.block_number,
            metadata.circuit_id,
            metadata.aggregation_round
        );

        match result {
            Ok(proof_wrapper) => {
                let mut connection = self
                    .connection_pool
                    .connection()
                    .await
                    .context("failed to get db connection")?;

                let is_scheduler_proof = metadata.is_scheduler_proof()?;

                let blob_url = self
                    .object_store
                    .put(metadata.id, &proof_wrapper)
                    .await
                    .context("failed to upload to object store")?;

                let mut transaction = connection
                    .start_transaction()
                    .await
                    .context("failed to start db transaction")?;
                transaction
                    .fri_prover_jobs_dal()
                    .save_proof(metadata.id, metadata.pick_time.elapsed(), &blob_url)
                    .await;
                if is_scheduler_proof {
                    transaction
                        .fri_proof_compressor_dal()
                        .insert_proof_compression_job(
                            metadata.block_number,
                            &blob_url,
                            self.protocol_version,
                        )
                        .await;
                }
                transaction
                    .commit()
                    .await
                    .context("failed to commit db transaction")?;
            }
            Err(error) => {
                let error_message = error.to_string();
                tracing::error!("GPU circuit prover failed: {:?}", error_message);
                self.connection_pool
                    .connection()
                    .await
                    .context("failed to get db connection")?
                    .fri_prover_jobs_dal()
                    .save_proof_error(metadata.id, error_message)
                    .await;
            }
        };
        tracing::info!(
            "Finished saving gpu circuit prover job {}, on batch {}, for circuit {}, at round {} after {:?}",
            metadata.id,
            metadata.block_number,
            metadata.circuit_id,
            metadata.aggregation_round,
            start_time.elapsed()
        );
        CIRCUIT_PROVER_METRICS
            .save_time
            .observe(start_time.elapsed());
        CIRCUIT_PROVER_METRICS
            .full_time
            .observe(metadata.pick_time.elapsed());
        Ok(())
    }
}

pub fn proxy_prover_runner(
    connection_pool: ConnectionPool<Prover>,
    object_store: Arc<dyn ObjectStore>,
    protocol_version: ProtocolSemanticVersion,
    setup_data_cache: HashMap<ProverServiceDataKey, Arc<GoldilocksGpuProverSetupData>>,
    receiver: mpsc::Receiver<(WitnessVectorGeneratorExecutionOutput, FriProverJobMetadata)>,
    grpc_sender: mpsc::Sender<lagrange_grpc::SubmitTaskRequest>,
    grpc_receiver: mpsc::Receiver<lagrange_grpc::SubmitTaskResponse>,
) -> JobRunner<ProxyExecutor, ProxyCircuitProverJobPicker, ProxyCircuitProverJobSaver> {
    let job_picker = ProxyCircuitProverJobPicker::new(receiver, setup_data_cache);
    let job_saver =
        ProxyCircuitProverJobSaver::new(connection_pool, object_store, protocol_version);
    JobRunner::new(
        ProxyExecutor::new(grpc_sender, grpc_receiver),
        job_picker,
        job_saver,
        1,
        None,
    )
}
