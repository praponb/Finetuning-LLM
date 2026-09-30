WITH cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        method,
        amount,
        paid_at,
        status,
        TO_DATE(paid_at) AS paid_at_date,
        COALESCE(method, 'DIRECT') AS method_filled
    FROM payments
    WHERE paid_at > TIMESTAMP '2023-12-15 06:00:00'
      AND method IS NOT NULL
),
metrics AS (
    -- Ranking and gap features
    SELECT
        id,
        method,
        amount,
        paid_at,
        status,
        paid_at_date,
        method_filled,
        LEAD(amount, 1, 0) OVER (PARTITION BY status ORDER BY amount ASC, id ASC) AS next_amount,
        PERCENT_RANK() OVER (PARTITION BY method ORDER BY amount DESC) AS amount_percent_rank
    FROM cleaned
),
matched_keys AS (
    -- Set difference on keys
    SELECT id
    FROM metrics
    EXCEPT
    SELECT id
    FROM payments
    WHERE HOUR(paid_at) BETWEEN 6 AND 10
),
reconciled AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM metrics s
    INNER JOIN matched_keys k ON s.id = k.id
),
scored AS (
    -- Running and moving aggregates
    SELECT
        *,
        RANK() OVER (PARTITION BY status ORDER BY amount_percent_rank ASC NULLS FIRST) AS amount_percent_rank_rank,
        ROUND(next_amount / SUM(next_amount) OVER (PARTITION BY status), 4) AS next_amount_share,
        RANK() OVER (PARTITION BY status ORDER BY next_amount DESC) AS next_amount_rank,
        SUM(next_amount) OVER (PARTITION BY method) AS method_sum_next_amount
    FROM reconciled
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROUND(method_sum_next_amount / SUM(method_sum_next_amount) OVER (PARTITION BY method, status), 4) AS method_sum_next_amount_share,
        MAX(amount_percent_rank_rank) OVER (PARTITION BY method, status ORDER BY CAST(paid_at AS BIGINT) ASC RANGE BETWEEN 7776000 PRECEDING AND CURRENT ROW) AS amount_percent_rank_rank_max_90d
    FROM scored
),
base AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        method,
        amount,
        paid_at,
        status,
        method_filled,
        amount_percent_rank_rank,
        next_amount_share,
        method_sum_next_amount,
        method_sum_next_amount_share,
        WEEKOFYEAR(paid_at) AS paid_at_weekofyear
    FROM with_windows
    WHERE method IS NOT NULL
),
totals AS (
    -- Multi-level aggregation
    SELECT
        status,
        COUNT_IF(status = 'FAILED') AS failed_events,
        ROUND(AVG(next_amount_share), 2) AS avg_next_amount_share,
        PERCENTILE_APPROX(method_sum_next_amount_share, ARRAY(0.25, 0.5, 0.75)) AS method_sum_next_amount_share_quartiles,
        COUNT(DISTINCT method) AS distinct_methods
    FROM base
    GROUP BY CUBE (status)
)
SELECT
    failed_events,
    avg_next_amount_share,
    method_sum_next_amount_share_quartiles,
    distinct_methods
FROM totals
ORDER BY distinct_methods DESC NULLS LAST, failed_events DESC;
