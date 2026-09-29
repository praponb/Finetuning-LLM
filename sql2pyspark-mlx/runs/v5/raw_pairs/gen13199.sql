WITH cleaned AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        customer_id,
        end_date,
        ROUND(monthly_fee, 2) monthly_fee_rounded
    FROM subscriptions
    WHERE end_date > DATE '2023-09-15'
),
scored AS (
    -- Ranking and gap features
    SELECT
        *,
        SUM(monthly_fee_rounded) OVER (ORDER BY end_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) cumulative_monthly_fee_rounded,
        FIRST_VALUE(monthly_fee_rounded) IGNORE NULLS OVER (ORDER BY end_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) first_known_monthly_fee_rounded
    FROM cleaned
    WHERE end_date >= '2025-10-01'
),
remaining_keys AS (
    -- Set intersection on keys
    SELECT id, customer_id
    FROM scored
    INTERSECT
    SELECT id, customer_id
    FROM subscriptions
    WHERE plan != 'enterprise'
),
kept_rows AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM scored s
    INNER JOIN remaining_keys k ON s.id = k.id AND s.customer_id = k.customer_id
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        *,
        PERCENT_RANK() OVER w_ordered cumulative_monthly_fee_rounded_percent_rank,
        MIN(cumulative_monthly_fee_rounded) OVER w_hist customer_id_min_cumulative_monthly_fee_rounded,
        RANK() OVER w_ordered cumulative_monthly_fee_rounded_rank
    FROM kept_rows
    WINDOW w_ordered AS (PARTITION BY customer_id ORDER BY cumulative_monthly_fee_rounded DESC NULLS LAST), w_hist AS (PARTITION BY customer_id)
),
base AS (
    -- Basic cleansing and defaults
    SELECT
        customer_id,
        end_date,
        cumulative_monthly_fee_rounded,
        first_known_monthly_fee_rounded,
        cumulative_monthly_fee_rounded_rank,
        LEAST(cumulative_monthly_fee_rounded_rank, customer_id_min_cumulative_monthly_fee_rounded) least_cumulative_monthly_fee_rounded_rank_customer_id_min_cumulative_monthly_fee_rounded,
        QUARTER(end_date) end_date_quarter
    FROM with_windows
    WHERE customer_id_min_cumulative_monthly_fee_rounded BETWEEN 170 AND 340
      AND end_date >= TO_DATE('2023-10-21')
),
stats AS (
    -- Multi-level aggregation
    SELECT
        CASE WHEN cumulative_monthly_fee_rounded >= 305 THEN 'HIGH' ELSE 'LOW' END AS cumulative_monthly_fee_rounded_tier,
        COUNT(*) AS cnt,
        COUNT(DISTINCT customer_id) AS distinct_customer_ids,
        SUM(end_date_quarter) AS total_end_date_quarter
    FROM base
    GROUP BY 1
)
SELECT
    cumulative_monthly_fee_rounded_tier,
    distinct_customer_ids,
    total_end_date_quarter
FROM stats
ORDER BY cumulative_monthly_fee_rounded_tier DESC
