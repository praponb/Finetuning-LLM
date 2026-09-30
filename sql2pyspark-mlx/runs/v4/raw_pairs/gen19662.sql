WITH matched_keys AS (
    -- Set difference on keys
    SELECT id, status
    FROM payments
    EXCEPT
    SELECT id, status
    FROM payments
    WHERE amount > 590
),
kept_rows AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM payments s
    INNER JOIN matched_keys k ON s.id = k.id AND s.status = k.status
),
valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, LOWER(status) AS status_lower
    FROM kept_rows
    WHERE status = 'FAILED'
),
scored AS (
    -- Ranking and gap features
    SELECT
        *,
        PERCENT_RANK() OVER (PARTITION BY status ORDER BY amount NULLS FIRST) AS amount_percent_rank,
        SUM(amount) OVER (ORDER BY CAST(paid_at AS BIGINT) RANGE BETWEEN 604800 PRECEDING AND CURRENT ROW) AS amount_sum_7d
    FROM valid
),
valid_payments AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        method,
        amount,
        status,
        status_lower,
        amount_sum_7d,
        amount_sum_7d + amount AS amount_sum_7d_plus_amount,
        TO_DATE(paid_at) AS paid_at_date
    FROM scored
    WHERE paid_at > TIMESTAMP '2024-04-18 08:00:00'
),
cleaned AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        SUBSTRING(method, 1, 2) AS method_prefix,
        COALESCE(amount_sum_7d, 0) AS amount_sum_7d_filled
    FROM valid_payments
    WHERE amount_sum_7d <= 6982
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        *,
        DENSE_RANK() OVER (PARTITION BY status ORDER BY amount_sum_7d_plus_amount DESC) AS amount_sum_7d_plus_amount_dense_rank,
        RANK() OVER w_ordered AS amount_sum_7d_filled_rank,
        ROUND(amount_sum_7d / SUM(amount_sum_7d) OVER (PARTITION BY status), 4) AS amount_sum_7d_share,
        DENSE_RANK() OVER w_ordered AS amount_sum_7d_filled_dense_rank,
        STDDEV_SAMP(amount) OVER w_hist AS method_stddev_samp_amount
    FROM cleaned
    WINDOW w_ordered AS (ORDER BY amount_sum_7d_filled DESC NULLS FIRST), w_hist AS (PARTITION BY method)
),
scored_payments AS (
    -- Ranking and gap features
    SELECT
        *,
        ROUND(amount / SUM(amount) OVER (PARTITION BY status), 4) AS amount_share,
        PERCENT_RANK() OVER (ORDER BY amount_sum_7d_filled_dense_rank DESC NULLS FIRST) AS amount_sum_7d_filled_dense_rank_percent_rank,
        RANK() OVER (PARTITION BY status ORDER BY amount_sum_7d_filled_rank DESC NULLS FIRST) AS amount_sum_7d_filled_rank_rank,
        AVG(amount_sum_7d_plus_amount_dense_rank) OVER (PARTITION BY method, status) AS method_avg_amount_sum_7d_plus_amount_dense_rank
    FROM with_windows
)
SELECT
    method,
    status,
    paid_at_date,
    method_prefix,
    amount_sum_7d_filled,
    amount_sum_7d_plus_amount_dense_rank,
    amount_sum_7d_share,
    amount_sum_7d_filled_rank_rank,
    method_avg_amount_sum_7d_plus_amount_dense_rank
FROM scored_payments
