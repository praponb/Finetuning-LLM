WITH valid AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        account_id,
        txn_type,
        amount,
        is_flagged,
        LENGTH(merchant) AS merchant_length
    FROM transactions
    WHERE txn_time > DATE '2023-04-21'
      AND amount BETWEEN 1200 AND 1700
),
scored AS (
    -- Ranking and gap features
    SELECT
        *,
        STDDEV_SAMP(merchant_length) OVER (PARTITION BY account_id, txn_type) AS account_id_stddev_samp_merchant_length,
        FIRST_VALUE(merchant_length, TRUE) OVER (PARTITION BY account_id ORDER BY merchant_length, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_merchant_length,
        SUM(amount) OVER (PARTITION BY txn_type ORDER BY merchant_length, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_amount
    FROM valid
    WHERE is_flagged = TRUE
),
scored_transactions AS (
    -- Running and moving aggregates
    SELECT
        *,
        RANK() OVER w_running AS account_id_stddev_samp_merchant_length_rank,
        ROW_NUMBER() OVER w_ordered AS event_seq
    FROM scored
    WINDOW w_running AS (PARTITION BY txn_type ORDER BY account_id_stddev_samp_merchant_length), w_ordered AS (PARTITION BY account_id ORDER BY merchant_length DESC, id)
),
cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        account_id,
        merchant_length,
        account_id_stddev_samp_merchant_length,
        running_amount,
        account_id_stddev_samp_merchant_length_rank,
        event_seq,
        IF(is_flagged, 'Yes', 'No') AS is_flagged_label
    FROM scored_transactions
    WHERE txn_type != 'CREDIT'
),
filtered AS (
    -- Keep recent, valid rows
    SELECT *, CASE WHEN account_id_stddev_samp_merchant_length >= 60 THEN 'High' WHEN account_id_stddev_samp_merchant_length >= 34 THEN 'Medium' ELSE 'Low' END AS account_id_stddev_samp_merchant_length_band
    FROM cleaned
    WHERE account_id_stddev_samp_merchant_length_rank < 4
)
SELECT
    id,
    merchant_length,
    account_id_stddev_samp_merchant_length,
    event_seq,
    is_flagged_label,
    account_id_stddev_samp_merchant_length_band
FROM filtered
ORDER BY id DESC
