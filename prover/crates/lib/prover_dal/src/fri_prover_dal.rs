#![doc = include_str!("../doc/FriProverDal.md")]
use std::{
    collections::HashMap,
    convert::TryFrom,
    str::FromStr,
    time::{Duration, Instant},
};

use sqlx::QueryBuilder;
use zksync_basic_types::{
    basic_fri_types::{
        AggregationRound, CircuitProverStatsEntry, ProtocolVersionedCircuitProverStats,
    },
    protocol_version::{ProtocolSemanticVersion, ProtocolVersionId, VersionPatch},
    prover_dal::{
        FriProverJobMetadata, JobCountStatistics, ProverJobFriInfo, ProverJobStatus, StuckJobs,
    },
    L1BatchNumber,
};
use zksync_db_connection::{
    connection::Connection, instrument::InstrumentExt, metrics::MethodLatency,
};

use crate::{duration_to_naive_time, pg_interval_from_duration, Prover};

#[derive(Debug)]
pub struct FriProverDal<'a, 'c> {
    pub(crate) storage: &'a mut Connection<'c, Prover>,
}

impl FriProverDal<'_, '_> {
    // Postgres has a limit of 65535 push_bind parameters per query.
    // We need to split the insert into chunks to avoid hitting this limit.
    // A single row in insert_prover_jobs push_binds 10 parameters, therefore
    // the limit is 65k / 10 ~ 6500 jobs chunk.
    const INSERT_JOBS_CHUNK_SIZE: usize = 6500;

    pub async fn insert_prover_jobs(
        &mut self,
        l1_batch_number: L1BatchNumber,
        circuit_ids_and_urls: Vec<(u8, String)>,
        aggregation_round: AggregationRound,
        depth: u16,
        protocol_version_id: ProtocolSemanticVersion,
    ) {
        let _latency = MethodLatency::new("save_fri_prover_jobs");
        if circuit_ids_and_urls.is_empty() {
            return;
        }

        for (chunk_index, chunk) in circuit_ids_and_urls
            .chunks(Self::INSERT_JOBS_CHUNK_SIZE)
            .enumerate()
        {
            // Build multi-row INSERT for the current chunk
            let mut query_builder = QueryBuilder::new(
                r#"
                INSERT INTO prover_jobs_fri (
                    l1_batch_number,
                    circuit_id,
                    circuit_blob_url,
                    aggregation_round,
                    sequence_number,
                    depth,
                    is_node_final_proof,
                    protocol_version,
                    status,
                    created_at,
                    updated_at,
                    protocol_version_patch
                )
                "#,
            );

            query_builder.push_values(
                chunk.iter().enumerate(),
                |mut row, (i, (circuit_id, circuit_blob_url))| {
                    row.push_bind(l1_batch_number.0 as i64)
                        .push_bind(*circuit_id as i16)
                        .push_bind(circuit_blob_url)
                        .push_bind(aggregation_round as i64)
                        .push_bind((chunk_index * Self::INSERT_JOBS_CHUNK_SIZE + i) as i64) // sequence_number
                        .push_bind(depth as i32)
                        .push_bind(false) // is_node_final_proof
                        .push_bind(protocol_version_id.minor as i32)
                        .push_bind("queued") // status
                        .push("NOW()") // created_at
                        .push("NOW()") // updated_at
                        .push_bind(protocol_version_id.patch.0 as i32);
                },
            );

            // Add the ON CONFLICT clause
            query_builder.push(
                r#"
                ON CONFLICT (l1_batch_number, aggregation_round, circuit_id, depth, sequence_number)
                DO UPDATE
                SET updated_at = NOW()
                "#,
            );

            // Execute the built query
            let query = query_builder.build();
            query.execute(self.storage.conn()).await.unwrap();
        }
    }

    /// Retrieves the next prover job to be proven. Called by WVGs.
    ///
    /// Prover jobs must be thought of as ordered.
    /// Prover must prioritize proving such jobs that will make the chain move forward the fastest.
    /// Current ordering:
    /// - pick the lowest batch
    /// - within the lowest batch, look at the lowest aggregation level (move up the proof tree)
    /// - pick the same type of circuit for as long as possible, this maximizes GPU cache reuse
    ///
    /// Most of this function is similar to `get_light_job()`.
    /// The 2 differ in the type of jobs they will load. Node jobs are heavy in resource utilization.
    ///
    /// NOTE: This function retrieves only node jobs.
    pub async fn get_heavy_job(
        &mut self,
        protocol_version: ProtocolSemanticVersion,
        picked_by: &str,
    ) -> Option<FriProverJobMetadata> {
        sqlx::query!(
            r#"
            UPDATE prover_jobs_fri
            SET
                status = 'in_progress',
                attempts = attempts + 1,
                updated_at = NOW(),
                processing_started_at = NOW(),
                picked_by = $3
            WHERE
                id = (
                    SELECT
                        id
                    FROM
                        prover_jobs_fri
                    WHERE
                        status = 'queued'
                        AND protocol_version = $1
                        AND protocol_version_patch = $2
                        AND aggregation_round = $4
                    ORDER BY
                        l1_batch_number ASC,
                        circuit_id ASC,
                        id ASC
                    LIMIT
                        1
                    FOR UPDATE
                    SKIP LOCKED
                )
            RETURNING
            prover_jobs_fri.id,
            prover_jobs_fri.l1_batch_number,
            prover_jobs_fri.circuit_id,
            prover_jobs_fri.aggregation_round,
            prover_jobs_fri.sequence_number,
            prover_jobs_fri.depth,
            prover_jobs_fri.is_node_final_proof
            "#,
            protocol_version.minor as i32,
            protocol_version.patch.0 as i32,
            picked_by,
            AggregationRound::NodeAggregation as i64,
        )
        .fetch_optional(self.storage.conn())
        .await
        .expect("failed to get prover job")
        .map(|row| FriProverJobMetadata {
            id: row.id as u32,
            block_number: L1BatchNumber(row.l1_batch_number as u32),
            circuit_id: row.circuit_id as u8,
            aggregation_round: AggregationRound::try_from(i32::from(row.aggregation_round))
                .unwrap(),
            sequence_number: row.sequence_number as usize,
            depth: row.depth as u16,
            is_node_final_proof: row.is_node_final_proof,
            pick_time: Instant::now(),
        })
    }

    /// Retrieves the next prover job to be proven. Called by WVGs.
    ///
    /// Prover jobs must be thought of as ordered.
    /// Prover must prioritize proving such jobs that will make the chain move forward the fastest.
    /// Current ordering:
    /// - pick the lowest batch
    /// - within the lowest batch, look at the lowest aggregation level (move up the proof tree)
    /// - pick the same type of circuit for as long as possible, this maximizes GPU cache reuse
    ///
    /// Most of this function is similar to `get_heavy_job()`.
    /// The 2 differ in the type of jobs they will load. Node jobs are heavy in resource utilization.
    ///
    /// NOTE: This function retrieves all jobs but nodes.
    pub async fn get_light_job(
        &mut self,
        protocol_version: ProtocolSemanticVersion,
        picked_by: &str,
    ) -> Option<FriProverJobMetadata> {
        sqlx::query!(
            r#"
            UPDATE prover_jobs_fri
            SET
                status = 'in_progress',
                attempts = attempts + 1,
                updated_at = NOW(),
                processing_started_at = NOW(),
                picked_by = $3
            WHERE
                id = (
                    SELECT
                        id
                    FROM
                        prover_jobs_fri
                    WHERE
                        status = 'queued'
                        AND protocol_version = $1
                        AND protocol_version_patch = $2
                        AND aggregation_round != $4
                    ORDER BY
                        l1_batch_number ASC,
                        aggregation_round ASC,
                        circuit_id ASC,
                        id ASC
                    LIMIT
                        1
                    FOR UPDATE
                    SKIP LOCKED
                )
            RETURNING
            prover_jobs_fri.id,
            prover_jobs_fri.l1_batch_number,
            prover_jobs_fri.circuit_id,
            prover_jobs_fri.aggregation_round,
            prover_jobs_fri.sequence_number,
            prover_jobs_fri.depth,
            prover_jobs_fri.is_node_final_proof
            "#,
            protocol_version.minor as i32,
            protocol_version.patch.0 as i32,
            picked_by,
            AggregationRound::NodeAggregation as i64
        )
        .fetch_optional(self.storage.conn())
        .await
        .expect("failed to get prover job")
        .map(|row| FriProverJobMetadata {
            id: row.id as u32,
            block_number: L1BatchNumber(row.l1_batch_number as u32),
            circuit_id: row.circuit_id as u8,
            aggregation_round: AggregationRound::try_from(i32::from(row.aggregation_round))
                .unwrap(),
            sequence_number: row.sequence_number as usize,
            depth: row.depth as u16,
            is_node_final_proof: row.is_node_final_proof,
            pick_time: Instant::now(),
        })
    }

    pub async fn save_proof_error(&mut self, id: u32, error: String) {
        {
            sqlx::query!(
                r#"
                UPDATE prover_jobs_fri
                SET
                    status = 'failed',
                    error = $1,
                    updated_at = NOW()
                WHERE
                    id = $2
                    AND status != 'successful'
                "#,
                error,
                i64::from(id)
            )
            .execute(self.storage.conn())
            .await
            .unwrap();
        }
    }

    pub async fn save_proof(
        &mut self,
        id: u32,
        time_taken: Duration,
        blob_url: &str,
    ) -> FriProverJobMetadata {
        sqlx::query!(
            r#"
            UPDATE prover_jobs_fri
            SET
                status = 'successful',
                updated_at = NOW(),
                time_taken = $1,
                proof_blob_url = $2
            WHERE
                id = $3
            RETURNING
            prover_jobs_fri.id,
            prover_jobs_fri.l1_batch_number,
            prover_jobs_fri.circuit_id,
            prover_jobs_fri.aggregation_round,
            prover_jobs_fri.sequence_number,
            prover_jobs_fri.depth,
            prover_jobs_fri.is_node_final_proof
            "#,
            duration_to_naive_time(time_taken),
            blob_url,
            i64::from(id)
        )
        .instrument("save_fri_proof")
        .report_latency()
        .with_arg("id", &id)
        .fetch_optional(self.storage)
        .await
        .unwrap()
        .map(|row| FriProverJobMetadata {
            id: row.id as u32,
            block_number: L1BatchNumber(row.l1_batch_number as u32),
            circuit_id: row.circuit_id as u8,
            aggregation_round: AggregationRound::try_from(i32::from(row.aggregation_round))
                .unwrap(),
            sequence_number: row.sequence_number as usize,
            depth: row.depth as u16,
            is_node_final_proof: row.is_node_final_proof,
            pick_time: Instant::now(),
        })
        .unwrap()
    }

    pub async fn requeue_stuck_jobs(
        &mut self,
        processing_timeout: Duration,
        max_attempts: u32,
    ) -> Vec<StuckJobs> {
        let processing_timeout = pg_interval_from_duration(processing_timeout);
        {
            sqlx::query!(
                r#"
                UPDATE prover_jobs_fri
                SET
                    status = 'queued',
                    updated_at = NOW(),
                    processing_started_at = NOW()
                WHERE
                    id IN (
                        SELECT
                            id
                        FROM
                            prover_jobs_fri
                        WHERE
                            (
                                status IN ('in_progress', 'in_gpu_proof')
                                AND processing_started_at <= NOW() - $1::INTERVAL
                                AND attempts < $2
                            )
                            OR (
                                status = 'failed'
                                AND attempts < $2
                            )
                        FOR UPDATE
                        SKIP LOCKED
                    )
                RETURNING
                id,
                status,
                attempts,
                circuit_id,
                error,
                picked_by
                "#,
                &processing_timeout,
                max_attempts as i32,
            )
            .fetch_all(self.storage.conn())
            .await
            .unwrap()
            .into_iter()
            .map(|row| StuckJobs {
                id: row.id as u64,
                status: row.status,
                attempts: row.attempts as u64,
                circuit_id: Some(row.circuit_id as u32),
                error: row.error,
                picked_by: row.picked_by,
            })
            .collect()
        }
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn insert_prover_job(
        &mut self,
        l1_batch_number: L1BatchNumber,
        circuit_id: u8,
        depth: u16,
        sequence_number: usize,
        aggregation_round: AggregationRound,
        circuit_blob_url: &str,
        is_node_final_proof: bool,
        protocol_version: ProtocolSemanticVersion,
    ) {
        sqlx::query!(
            r#"
            INSERT INTO
            prover_jobs_fri (
                l1_batch_number,
                circuit_id,
                circuit_blob_url,
                aggregation_round,
                sequence_number,
                depth,
                is_node_final_proof,
                protocol_version,
                status,
                created_at,
                updated_at,
                protocol_version_patch
            )
            VALUES
            ($1, $2, $3, $4, $5, $6, $7, $8, 'queued', NOW(), NOW(), $9)
            ON CONFLICT (
                l1_batch_number, aggregation_round, circuit_id, depth, sequence_number
            ) DO
            UPDATE
            SET
            updated_at = NOW()
            "#,
            i64::from(l1_batch_number.0),
            i16::from(circuit_id),
            circuit_blob_url,
            aggregation_round as i64,
            sequence_number as i64,
            i32::from(depth),
            is_node_final_proof,
            protocol_version.minor as i32,
            protocol_version.patch.0 as i32,
        )
        .execute(self.storage.conn())
        .await
        .unwrap();
    }

    pub async fn get_prover_jobs_stats(&mut self) -> ProtocolVersionedCircuitProverStats {
        {
            sqlx::query!(
                r#"
                SELECT
                    COUNT(*) AS "count!",
                    circuit_id AS "circuit_id!",
                    aggregation_round AS "aggregation_round!",
                    status AS "status!",
                    protocol_version AS "protocol_version!",
                    protocol_version_patch AS "protocol_version_patch!"
                FROM
                    prover_jobs_fri
                WHERE
                    (
                        status = 'queued'
                        OR status = 'in_progress'
                    )
                    AND protocol_version IS NOT NULL
                GROUP BY
                    circuit_id,
                    aggregation_round,
                    status,
                    protocol_version,
                    protocol_version_patch
                "#
            )
            .fetch_all(self.storage.conn())
            .await
            .unwrap()
            .iter()
            .map(|row| {
                CircuitProverStatsEntry::new(
                    row.circuit_id,
                    row.aggregation_round,
                    row.protocol_version,
                    row.protocol_version_patch,
                    &row.status,
                    row.count,
                )
            })
            .collect()
        }
    }

    pub async fn get_generic_prover_jobs_stats(
        &mut self,
    ) -> HashMap<ProtocolSemanticVersion, JobCountStatistics> {
        {
            sqlx::query!(
                r#"
                SELECT
                    protocol_version AS "protocol_version!",
                    protocol_version_patch AS "protocol_version_patch!",
                    COUNT(*) FILTER (
                        WHERE
                        status = 'queued'
                    ) AS queued,
                    COUNT(*) FILTER (
                        WHERE
                        status = 'in_progress'
                    ) AS in_progress
                FROM
                    prover_jobs_fri
                WHERE
                    status IN ('queued', 'in_progress')
                    AND protocol_version IS NOT NULL
                GROUP BY
                    protocol_version,
                    protocol_version_patch
                "#
            )
            .fetch_all(self.storage.conn())
            .await
            .unwrap()
            .into_iter()
            .map(|row| {
                let protocol_semantic_version = ProtocolSemanticVersion::new(
                    ProtocolVersionId::try_from(row.protocol_version as u16).unwrap(),
                    VersionPatch(row.protocol_version_patch as u32),
                );
                let key = protocol_semantic_version;
                let value = JobCountStatistics {
                    queued: row.queued.unwrap() as usize,
                    in_progress: row.in_progress.unwrap() as usize,
                };
                (key, value)
            })
            .collect()
        }
    }

    pub async fn min_unproved_l1_batch_number(&mut self) -> HashMap<(u8, u8), L1BatchNumber> {
        {
            sqlx::query!(
                r#"
                SELECT
                    MIN(l1_batch_number) AS "l1_batch_number!",
                    circuit_id,
                    aggregation_round
                FROM
                    prover_jobs_fri
                WHERE
                    status IN ('queued', 'in_gpu_proof', 'in_progress', 'failed')
                GROUP BY
                    circuit_id,
                    aggregation_round
                "#
            )
            .fetch_all(self.storage.conn())
            .await
            .unwrap()
            .into_iter()
            .map(|row| {
                (
                    (row.circuit_id as u8, row.aggregation_round as u8),
                    L1BatchNumber(row.l1_batch_number as u32),
                )
            })
            .collect()
        }
    }

    pub async fn update_status(&mut self, id: u32, status: &str) {
        sqlx::query!(
            r#"
            UPDATE prover_jobs_fri
            SET
                status = $1,
                updated_at = NOW()
            WHERE
                id = $2
                AND status != 'successful'
            "#,
            status,
            i64::from(id)
        )
        .execute(self.storage.conn())
        .await
        .unwrap();
    }

    pub async fn get_scheduler_proof_job_id(
        &mut self,
        l1_batch_number: L1BatchNumber,
    ) -> Option<u32> {
        sqlx::query!(
            r#"
            SELECT
                id
            FROM
                prover_jobs_fri
            WHERE
                l1_batch_number = $1
                AND status = 'successful'
                AND aggregation_round = $2
            "#,
            i64::from(l1_batch_number.0),
            AggregationRound::Scheduler as i16,
        )
        .fetch_optional(self.storage.conn())
        .await
        .ok()?
        .map(|row| row.id as u32)
    }

    pub async fn get_recursion_tip_proof_job_id(
        &mut self,
        l1_batch_number: L1BatchNumber,
    ) -> Option<u32> {
        sqlx::query!(
            r#"
            SELECT
                id
            FROM
                prover_jobs_fri
            WHERE
                l1_batch_number = $1
                AND status = 'successful'
                AND aggregation_round = $2
            "#,
            l1_batch_number.0 as i64,
            AggregationRound::RecursionTip as i16,
        )
        .fetch_optional(self.storage.conn())
        .await
        .ok()?
        .map(|row| row.id as u32)
    }
    pub async fn archive_old_jobs(&mut self, archiving_interval: Duration) -> usize {
        let archiving_interval_secs = pg_interval_from_duration(archiving_interval);

        sqlx::query_scalar!(
            r#"
            WITH deleted AS (
                DELETE FROM prover_jobs_fri AS p
                USING proof_compression_jobs_fri AS c
                WHERE
                    p.status NOT IN ('queued', 'in_progress', 'in_gpu_proof', 'failed')
                    AND p.updated_at < NOW() - $1::INTERVAL
                    AND p.l1_batch_number = c.l1_batch_number
                    AND c.status = 'sent_to_server'
                RETURNING p.*
            ),
            inserted_count AS (
                INSERT INTO prover_jobs_fri_archive
                SELECT * FROM deleted
            )
            SELECT COUNT(*) FROM deleted
            "#,
            &archiving_interval_secs,
        )
        .fetch_one(self.storage.conn())
        .await
        .unwrap()
        .unwrap_or(0) as usize
    }

    pub async fn get_final_node_proof_job_ids_for(
        &mut self,
        l1_batch_number: L1BatchNumber,
    ) -> Vec<(u8, u32)> {
        sqlx::query!(
            r#"
            SELECT
                circuit_id,
                id
            FROM
                prover_jobs_fri
            WHERE
                l1_batch_number = $1
                AND is_node_final_proof = TRUE
                AND status = 'successful'
            ORDER BY
                circuit_id ASC
            "#,
            l1_batch_number.0 as i64
        )
        .fetch_all(self.storage.conn())
        .await
        .unwrap()
        .into_iter()
        .map(|row| (row.circuit_id as u8, row.id as u32))
        .collect()
    }

    pub async fn get_prover_jobs_stats_for_batch(
        &mut self,
        l1_batch_number: L1BatchNumber,
        aggregation_round: AggregationRound,
    ) -> Vec<ProverJobFriInfo> {
        sqlx::query!(
            r#"
            SELECT
                *
            FROM
                prover_jobs_fri
            WHERE
                l1_batch_number = $1
                AND aggregation_round = $2
            "#,
            i64::from(l1_batch_number.0),
            aggregation_round as i16
        )
        .fetch_all(self.storage.conn())
        .await
        .unwrap()
        .iter()
        .map(|row| ProverJobFriInfo {
            id: row.id as u32,
            l1_batch_number,
            circuit_id: row.circuit_id as u32,
            circuit_blob_url: row.circuit_blob_url.clone(),
            aggregation_round,
            sequence_number: row.sequence_number as u32,
            status: ProverJobStatus::from_str(&row.status).unwrap(),
            error: row.error.clone(),
            attempts: row.attempts as u8,
            processing_started_at: row.processing_started_at,
            created_at: row.created_at,
            updated_at: row.updated_at,
            time_taken: row.time_taken,
            depth: row.depth as u32,
            is_node_final_proof: row.is_node_final_proof,
            proof_blob_url: row.proof_blob_url.clone(),
            protocol_version: row.protocol_version.map(|protocol_version| {
                ProtocolVersionId::try_from(protocol_version as u16).unwrap()
            }),
            picked_by: row.picked_by.clone(),
        })
        .collect()
    }

    pub async fn delete_prover_jobs_fri_batch_data(
        &mut self,
        l1_batch_number: L1BatchNumber,
    ) -> sqlx::Result<sqlx::postgres::PgQueryResult> {
        sqlx::query!(
            r#"
            DELETE FROM prover_jobs_fri
            WHERE
                l1_batch_number = $1;
            "#,
            i64::from(l1_batch_number.0)
        )
        .execute(self.storage.conn())
        .await
    }

    pub async fn delete_batch_data(
        &mut self,
        l1_batch_number: L1BatchNumber,
    ) -> sqlx::Result<sqlx::postgres::PgQueryResult> {
        self.delete_prover_jobs_fri_batch_data(l1_batch_number)
            .await
    }

    pub async fn delete_prover_jobs_fri(&mut self) -> sqlx::Result<sqlx::postgres::PgQueryResult> {
        sqlx::query!(
            r#"
            DELETE FROM prover_jobs_fri
            "#
        )
        .execute(self.storage.conn())
        .await
    }

    pub async fn delete(&mut self) -> sqlx::Result<sqlx::postgres::PgQueryResult> {
        self.delete_prover_jobs_fri().await
    }

    pub async fn requeue_stuck_jobs_for_batch(
        &mut self,
        block_number: L1BatchNumber,
        max_attempts: u32,
    ) -> Vec<StuckJobs> {
        {
            sqlx::query!(
                r#"
                UPDATE prover_jobs_fri
                SET
                    status = 'queued',
                    error = 'Manually requeued',
                    attempts = 2,
                    updated_at = NOW(),
                    processing_started_at = NOW()
                WHERE
                    l1_batch_number = $1
                    AND attempts >= $2
                    AND (
                        status = 'in_progress'
                        OR status = 'failed'
                    )
                RETURNING
                id,
                status,
                attempts,
                circuit_id,
                error,
                picked_by
                "#,
                i64::from(block_number.0),
                max_attempts as i32,
            )
            .fetch_all(self.storage.conn())
            .await
            .unwrap()
            .into_iter()
            .map(|row| StuckJobs {
                id: row.id as u64,
                status: row.status,
                attempts: row.attempts as u64,
                circuit_id: Some(row.circuit_id as u32),
                error: row.error,
                picked_by: row.picked_by,
            })
            .collect()
        }
    }

    pub async fn prover_job_ids_for(
        &mut self,
        block_number: L1BatchNumber,
        circuit_id: u8,
        round: AggregationRound,
        depth: u16,
    ) -> Vec<u32> {
        sqlx::query!(
            r#"
            SELECT
                id
            FROM
                prover_jobs_fri
            WHERE
                l1_batch_number = $1
                AND circuit_id = $2
                AND aggregation_round = $3
                AND depth = $4
                AND status = 'successful'
            ORDER BY
                sequence_number ASC;
            "#,
            i64::from(block_number.0),
            i16::from(circuit_id),
            round as i16,
            i32::from(depth)
        )
        .fetch_all(self.storage.conn())
        .await
        .unwrap()
        .into_iter()
        .map(|row| row.id as u32)
        .collect::<_>()
    }

    pub async fn check_reached_max_attempts(&mut self, max_attempts: u32) -> usize {
        sqlx::query_scalar!(
            r#"
            SELECT COUNT(*)
            FROM prover_jobs_fri
            WHERE
                attempts >= $1
                AND status <> 'successful'
            "#,
            max_attempts as i64
        )
        .fetch_one(self.storage.conn())
        .await
        .unwrap()
        .unwrap_or(0) as usize
    }
}

#[cfg(test)]
mod tests {
    use zksync_basic_types::protocol_version::L1VerifierConfig;
    use zksync_db_connection::connection_pool::ConnectionPool;

    use super::*;
    use crate::ProverDal;

    fn mock_circuit_ids_and_urls(num_circuits: usize) -> Vec<(u8, String)> {
        (0..num_circuits)
            .map(|i| (i as u8, format!("circuit{}", i)))
            .collect()
    }

    #[tokio::test]
    async fn test_insert_prover_jobs() {
        let pool = ConnectionPool::<Prover>::prover_test_pool().await;
        let mut conn = pool.connection().await.unwrap();
        let mut transaction = conn.start_transaction().await.unwrap();

        transaction
            .fri_protocol_versions_dal()
            .save_prover_protocol_version(
                ProtocolSemanticVersion::default(),
                L1VerifierConfig::default(),
            )
            .await
            .unwrap();
        transaction
            .fri_prover_jobs_dal()
            .insert_prover_jobs(
                L1BatchNumber(1),
                mock_circuit_ids_and_urls(10000),
                AggregationRound::Scheduler,
                1,
                ProtocolSemanticVersion::default(),
            )
            .await;

        transaction.commit().await.unwrap();
    }
}
