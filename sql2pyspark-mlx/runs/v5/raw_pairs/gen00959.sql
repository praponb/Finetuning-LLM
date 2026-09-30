WITH filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        DATEDIFF(delivered_date, shipped_date) AS days_shipped_date_to_delivered_date,
        CONCAT_WS('-', carrier, status) AS carrier_status_key,
        COALESCE(carrier, 'UNKNOWN') AS carrier_clean
    FROM shipments
    WHERE shipped_date >= DATE '2023-06-30'
),
deduped AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY carrier ORDER BY days_shipped_date_to_delivered_date, id) AS latest_rank
        FROM filtered
    ) ranked
    WHERE latest_rank = 1
),
enriched AS (
    -- Ranking and gap features
    SELECT
        id,
        order_id,
        delivered_date,
        days_shipped_date_to_delivered_date,
        carrier_status_key,
        latest_rank,
        NTILE(10) OVER (PARTITION BY status ORDER BY cost DESC, id) AS cost_bucket,
        PERCENT_RANK() OVER (ORDER BY days_shipped_date_to_delivered_date DESC NULLS LAST) AS days_shipped_date_to_delivered_date_percent_rank,
        LEAD(days_shipped_date_to_delivered_date, 1) OVER (PARTITION BY status, order_id ORDER BY delivered_date, id) AS next_days_shipped_date_to_delivered_date,
        ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY shipped_date DESC, id) AS row_seq
    FROM deduped
),
latest AS (
    -- QUALIFY-style window filter
    SELECT *, RANK() OVER (PARTITION BY order_id ORDER BY cost_bucket DESC NULLS FIRST) AS rn
    FROM enriched
    QUALIFY rn = 1
),
with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        SUM(row_seq) OVER (PARTITION BY order_id ORDER BY latest_rank, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_row_seq,
        LEAD(days_shipped_date_to_delivered_date, 1) OVER (PARTITION BY order_id ORDER BY latest_rank, id) AS next_days_shipped_date_to_delivered_date_2
    FROM latest
),
base AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        order_id,
        days_shipped_date_to_delivered_date,
        latest_rank,
        cost_bucket,
        days_shipped_date_to_delivered_date_percent_rank,
        next_days_shipped_date_to_delivered_date,
        row_seq,
        running_row_seq,
        ROUND(cost_bucket / latest_rank, 2) AS cost_bucket_per_latest_rank
    FROM with_windows
    WHERE delivered_date >= DATE '2023-03-20'
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        AVG(cost_bucket) OVER (PARTITION BY order_id ORDER BY days_shipped_date_to_delivered_date, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS cost_bucket_avg_3,
        NTILE(3) OVER (ORDER BY days_shipped_date_to_delivered_date_percent_rank, id) AS days_shipped_date_to_delivered_date_percent_rank_bucket
    FROM base
    WHERE latest_rank = 1
)
SELECT
    id,
    days_shipped_date_to_delivered_date,
    cost_bucket,
    next_days_shipped_date_to_delivered_date,
    row_seq,
    running_row_seq,
    cost_bucket_per_latest_rank,
    cost_bucket_avg_3,
    days_shipped_date_to_delivered_date_percent_rank_bucket
FROM metrics
ORDER BY days_shipped_date_to_delivered_date_percent_rank_bucket NULLS FIRST;
