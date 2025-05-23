use zksync_db_connection::{
    connection::Connection,
    error::DalResult,
    instrument::{InstrumentExt, Instrumented},
};
use zksync_types::{
    commitment::PubdataType,
    l2_to_l1_log::L2ToL1Log,
    pubdata_da::{DataAvailabilityBlob, DataAvailabilityDetails},
    Address, L1BatchNumber,
};

use crate::{
    models::storage_data_availability::{L1BatchDA, StorageDABlob, StorageDADetails},
    Core,
};

#[derive(Debug)]
pub struct DataAvailabilityDal<'a, 'c> {
    pub(crate) storage: &'a mut Connection<'c, Core>,
}

impl DataAvailabilityDal<'_, '_> {
    /// Inserts the blob_id for the given L1 batch. If the blob_id is already present,
    /// verifies that it matches the one provided in the function arguments
    /// (preventing the same L1 batch from being stored twice)
    pub async fn insert_l1_batch_da(
        &mut self,
        number: L1BatchNumber,
        blob_id: &str,
        sent_at: chrono::NaiveDateTime,
        pubdata_type: PubdataType,
        da_inclusion_data: Option<&[u8]>,
        l2_validator_address: Option<Address>,
    ) -> DalResult<()> {
        let update_result = sqlx::query!(
            r#"
            INSERT INTO
            data_availability (
                l1_batch_number,
                blob_id,
                inclusion_data,
                client_type,
                l2_da_validator_address,
                sent_at,
                created_at,
                updated_at
            )
            VALUES
            ($1, $2, $3, $4, $5, $6, NOW(), NOW())
            ON CONFLICT DO NOTHING
            "#,
            i64::from(number.0),
            blob_id,
            da_inclusion_data,
            pubdata_type.to_string(),
            l2_validator_address.map(|addr| addr.as_bytes().to_vec()),
            sent_at,
        )
        .instrument("insert_l1_batch_da")
        .with_arg("number", &number)
        .with_arg("blob_id", &blob_id)
        .report_latency()
        .execute(self.storage)
        .await?;

        if update_result.rows_affected() == 0 {
            tracing::debug!(
                "L1 batch #{number}: DA blob_id wasn't updated as it's already present"
            );

            let instrumentation =
                Instrumented::new("get_matching_batch_da_blob_id").with_arg("number", &number);

            // Batch was already processed. Verify that existing DA blob_id matches
            let query = sqlx::query!(
                r#"
                SELECT
                    blob_id
                FROM
                    data_availability
                WHERE
                    l1_batch_number = $1
                "#,
                i64::from(number.0),
            );

            let matched: String = instrumentation
                .clone()
                .with(query)
                .report_latency()
                .fetch_one(self.storage)
                .await?
                .blob_id;

            if matched != *blob_id.to_string() {
                let err = instrumentation.constraint_error(anyhow::anyhow!(
                    "Error storing DA blob id. DA blob_id {blob_id} for L1 batch #{number} does not match the expected value"
                ));
                return Err(err);
            }
        }
        Ok(())
    }

    /// Saves the inclusion data for the given L1 batch. If the inclusion data is already present,
    /// verifies that it matches the one provided in the function arguments
    /// (meaning that the inclusion data corresponds to the same DA blob)
    pub async fn save_l1_batch_inclusion_data(
        &mut self,
        number: L1BatchNumber,
        da_inclusion_data: &[u8],
    ) -> DalResult<()> {
        let update_result = sqlx::query!(
            r#"
            UPDATE data_availability
            SET
                inclusion_data = $1,
                updated_at = NOW()
            WHERE
                l1_batch_number = $2
                AND inclusion_data IS NULL
            "#,
            da_inclusion_data,
            i64::from(number.0),
        )
        .instrument("save_l1_batch_da_data")
        .with_arg("number", &number)
        .report_latency()
        .execute(self.storage)
        .await?;

        if update_result.rows_affected() == 0 {
            tracing::debug!("L1 batch #{number}: DA data wasn't updated as it's already present");

            let instrumentation =
                Instrumented::new("get_matching_batch_da_data").with_arg("number", &number);

            // Batch was already processed. Verify that existing DA data matches
            let query = sqlx::query!(
                r#"
                SELECT
                    inclusion_data
                FROM
                    data_availability
                WHERE
                    l1_batch_number = $1
                "#,
                i64::from(number.0),
            );

            let matched: Option<Vec<u8>> = instrumentation
                .clone()
                .with(query)
                .report_latency()
                .fetch_one(self.storage)
                .await?
                .inclusion_data;

            if matched.unwrap_or_default() != da_inclusion_data.to_vec() {
                let err = instrumentation.constraint_error(anyhow::anyhow!(
                    "Error storing DA inclusion data. DA data for L1 batch #{number} does not match the one provided before"
                ));
                return Err(err);
            }
        }
        Ok(())
    }

    /// Assumes that the L1 batches are sorted by number, and returns the first one that is ready for DA dispatch.
    pub async fn get_first_da_blob_awaiting_inclusion(
        &mut self,
    ) -> DalResult<Option<DataAvailabilityBlob>> {
        Ok(sqlx::query_as!(
            StorageDABlob,
            r#"
            SELECT
                l1_batch_number,
                blob_id,
                inclusion_data,
                sent_at
            FROM
                data_availability
            WHERE
                inclusion_data IS NULL
            ORDER BY
                l1_batch_number
            LIMIT
                1
            "#,
        )
        .instrument("get_first_da_blob_awaiting_inclusion")
        .fetch_optional(self.storage)
        .await?
        .map(DataAvailabilityBlob::from))
    }

    /// Fetches the pubdata and `l1_batch_number` for the L1 batches that are ready for DA dispatch.
    pub async fn get_ready_for_da_dispatch_l1_batches(
        &mut self,
        limit: usize,
    ) -> DalResult<Vec<L1BatchDA>> {
        let rows = sqlx::query!(
            r#"
            SELECT
                number,
                pubdata_input,
                system_logs,
                sealed_at
            FROM
                l1_batches
            LEFT JOIN
                data_availability
                ON data_availability.l1_batch_number = l1_batches.number
            WHERE
                eth_commit_tx_id IS NULL
                AND number != 0
                AND data_availability.blob_id IS NULL
                AND pubdata_input IS NOT NULL
                AND sealed_at IS NOT NULL
            ORDER BY
                number
            LIMIT
                $1
            "#,
            limit as i64,
        )
        .instrument("get_ready_for_da_dispatch_l1_batches")
        .with_arg("limit", &limit)
        .fetch_all(self.storage)
        .await?;

        Ok(rows
            .into_iter()
            .map(|row| L1BatchDA {
                // `unwrap` is safe here because we have a `WHERE` clause that filters out `NULL` values
                pubdata: row.pubdata_input.unwrap(),
                l1_batch_number: L1BatchNumber(row.number as u32),
                sealed_at: row.sealed_at.unwrap().and_utc(),
                system_logs: row
                    .system_logs
                    .into_iter()
                    .map(|raw_log| L2ToL1Log::from_slice(&raw_log))
                    .collect(),
            })
            .collect())
    }

    pub async fn get_da_details_by_batch_number(
        &mut self,
        number: L1BatchNumber,
    ) -> DalResult<Option<DataAvailabilityDetails>> {
        Ok(sqlx::query_as!(
            StorageDADetails,
            r#"
            SELECT
                blob_id,
                client_type,
                inclusion_data,
                sent_at,
                l2_da_validator_address
            FROM
                data_availability
            WHERE
                l1_batch_number = $1
            "#,
            i64::from(number.0),
        )
        .instrument("get_da_details_by_batch_number")
        .with_arg("number", &number)
        .fetch_optional(self.storage)
        .await?
        .map(DataAvailabilityDetails::from))
    }

    pub async fn get_latest_batch_with_inclusion_data(
        &mut self,
    ) -> DalResult<Option<L1BatchNumber>> {
        let row = sqlx::query!(
            r#"
            SELECT
                l1_batch_number
            FROM
                data_availability
            WHERE
                inclusion_data IS NOT NULL
            ORDER BY
                l1_batch_number DESC
            LIMIT
                1
            "#,
        )
        .instrument("get_latest_batch_with_inclusion_data")
        .report_latency()
        .fetch_optional(self.storage)
        .await?;

        Ok(row.map(|row| L1BatchNumber(row.l1_batch_number as u32)))
    }

    /// Fetches the pubdata for the L1 batch with a given blob id.
    pub async fn get_blob_data_by_blob_id(
        &mut self,
        blob_id: &str,
    ) -> DalResult<Option<L1BatchDA>> {
        let row = sqlx::query!(
            r#"
            SELECT
                number,
                pubdata_input,
                system_logs,
                sealed_at
            FROM
                l1_batches
            LEFT JOIN
                data_availability
                ON data_availability.l1_batch_number = l1_batches.number
            WHERE
                number != 0
                AND data_availability.blob_id = $1
            ORDER BY
                number
            LIMIT
                1
            "#,
            blob_id,
        )
        .instrument("get_blob_data_by_blob_id")
        .with_arg("blob_id", &blob_id)
        .fetch_optional(self.storage)
        .await?
        .map(|row| L1BatchDA {
            // `unwrap` is safe here because we have a `WHERE` clause that filters out `NULL` values
            pubdata: row.pubdata_input.unwrap(),
            l1_batch_number: L1BatchNumber(row.number as u32),
            sealed_at: row.sealed_at.unwrap().and_utc(),
            system_logs: row
                .system_logs
                .into_iter()
                .map(|raw_log| L2ToL1Log::from_slice(&raw_log))
                .collect(),
        });

        Ok(row)
    }

    pub async fn set_dummy_inclusion_data_for_old_batches(
        &mut self,
        current_l2_da_validator: Address,
    ) -> DalResult<()> {
        sqlx::query!(
            r#"
            UPDATE data_availability
            SET
                inclusion_data = $1,
                updated_at = NOW()
            WHERE
                inclusion_data IS NULL
                AND (l2_da_validator_address IS NULL OR l2_da_validator_address != $2)
            "#,
            vec![],
            current_l2_da_validator.as_bytes(),
        )
        .instrument("set_dummy_inclusion_data_for_old_batches")
        .report_latency()
        .execute(self.storage)
        .await?;

        Ok(())
    }
}
