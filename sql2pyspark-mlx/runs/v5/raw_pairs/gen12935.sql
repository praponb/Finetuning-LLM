WITH base AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        carrier,
        shipped_date,
        delivered_date,
        cost,
        CONCAT_WS('/', carrier, status) AS carrier_status_key,
        COALESCE(carrier, 'DIRECT') AS carrier_filled
    FROM shipments
    WHERE order_id IS NOT NULL
      AND carrier IS NOT DISTINCT FROM 'FedEx'
),
deduped AS (
    -- QUALIFY-style window filter
    SELECT *, ROW_NUMBER() OVER (PARTITION BY carrier ORDER BY delivered_date DESC NULLS LAST, id) AS rank_in_group
    FROM base
    QUALIFY rank_in_group = 1
),
cleaned AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        cost,
        carrier_filled,
        ADD_MONTHS(shipped_date, 1) AS shipped_date_plus_months,
        COALESCE(carrier, 'N/A') AS carrier_clean
    FROM deduped
    WHERE rank_in_group >= 1
),
metrics AS (
    -- Ranking and gap features
    SELECT
        *,
        NTILE(3) OVER (ORDER BY cost DESC, id) AS cost_bucket,
        CUME_DIST() OVER w_running AS cost_cume_dist
    FROM cleaned
    WHERE shipped_date_plus_months < '2024-01-01'
    WINDOW w_running AS (ORDER BY cost)
),
metrics_shipments AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        shipped_date_plus_months,
        cost_bucket,
        LEAD(cost_cume_dist) OVER (ORDER BY shipped_date_plus_months, id) AS next_cost_cume_dist,
        LAG(cost) OVER (ORDER BY shipped_date_plus_months, id) AS prev_cost,
        LEAD(cost) OVER (ORDER BY shipped_date_plus_months, id) AS next_cost,
        ROW_NUMBER() OVER (ORDER BY shipped_date_plus_months, id) AS event_seq,
        RANK() OVER (ORDER BY cost_bucket) AS cost_bucket_rank
    FROM metrics
),
scored AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        shipped_date_plus_months,
        next_cost_cume_dist,
        prev_cost,
        cost_bucket_rank,
        LAST_VALUE(cost_bucket) IGNORE NULLS OVER (ORDER BY cost_bucket, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_cost_bucket,
        AVG(event_seq) OVER w_recent AS event_seq_avg_5,
        NTILE(3) OVER (ORDER BY next_cost DESC, id) AS next_cost_bucket,
        AVG(cost_bucket_rank) OVER w_grp AS cost_bucket_rank_avg_3_prior,
        ROW_NUMBER() OVER (ORDER BY cost_bucket_rank, id) AS seq_no
    FROM metrics_shipments
    WINDOW w_recent AS (ORDER BY event_seq, id ROWS BETWEEN 5 PRECEDING AND CURRENT ROW), w_grp AS (ORDER BY cost_bucket_rank, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING)
)
SELECT
    id,
    shipped_date_plus_months,
    next_cost_cume_dist,
    prev_cost,
    cost_bucket_rank,
    last_known_cost_bucket,
    event_seq_avg_5,
    next_cost_bucket,
    cost_bucket_rank_avg_3_prior
FROM scored
