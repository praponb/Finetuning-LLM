WITH cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        customer_id,
        start_date,
        monthly_fee,
        status,
        WEEKOFYEAR(end_date) AS end_date_weekofyear,
        DATE_SUB(end_date, 86) AS end_date_minus_86d
    FROM subscriptions
    WHERE end_date > DATE '2023-10-31'
),
latest AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM cleaned
    QUALIFY ROW_NUMBER() OVER (PARTITION BY status, customer_id ORDER BY end_date_minus_86d DESC, id) = 1
),
scored AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        SUM(monthly_fee) OVER (PARTITION BY status ORDER BY start_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_monthly_fee,
        ROUND(monthly_fee / SUM(monthly_fee) OVER (PARTITION BY status), 4) AS monthly_fee_share
    FROM latest
),
latest_subscriptions AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM scored
    QUALIFY RANK() OVER (PARTITION BY customer_id, status ORDER BY cumulative_monthly_fee DESC NULLS LAST) = 1
),
filtered AS (
    -- Keep recent, valid rows
    SELECT
        *,
        ROUND(cumulative_monthly_fee * 100, 2) AS cumulative_monthly_fee_adjusted,
        ROUND(cumulative_monthly_fee * 1.15, 2) AS cumulative_monthly_fee_adjusted_2
    FROM latest_subscriptions
    WHERE start_date > DATE '2023-11-15'
      AND monthly_fee <= 180
),
metrics AS (
    -- Ranking and gap features
    SELECT
        id,
        customer_id,
        start_date,
        monthly_fee,
        status,
        end_date_weekofyear,
        end_date_minus_86d,
        cumulative_monthly_fee,
        monthly_fee_share,
        cumulative_monthly_fee_adjusted,
        cumulative_monthly_fee_adjusted_2,
        LAG(cumulative_monthly_fee, 1, 0) OVER (PARTITION BY customer_id ORDER BY cumulative_monthly_fee, id) AS prev_cumulative_monthly_fee,
        LAG(monthly_fee_share, 2) OVER (PARTITION BY status ORDER BY end_date_weekofyear, id) AS prev_monthly_fee_share,
        FIRST_VALUE(cumulative_monthly_fee) IGNORE NULLS OVER (ORDER BY cumulative_monthly_fee, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_cumulative_monthly_fee,
        LAST_VALUE(end_date_weekofyear) IGNORE NULLS OVER (PARTITION BY customer_id ORDER BY monthly_fee, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_end_date_weekofyear
    FROM filtered
),
metrics_subscriptions AS (
    -- Running and moving aggregates
    SELECT
        id,
        customer_id,
        start_date,
        monthly_fee,
        status,
        end_date_weekofyear,
        end_date_minus_86d,
        cumulative_monthly_fee,
        monthly_fee_share,
        cumulative_monthly_fee_adjusted,
        cumulative_monthly_fee_adjusted_2,
        prev_cumulative_monthly_fee,
        prev_monthly_fee_share,
        first_known_cumulative_monthly_fee,
        last_known_end_date_weekofyear,
        COUNT(cumulative_monthly_fee) OVER (PARTITION BY customer_id) AS customer_id_count_cumulative_monthly_fee,
        SUM(monthly_fee_share) OVER (PARTITION BY customer_id ORDER BY start_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_monthly_fee_share
    FROM metrics
)
SELECT
    id,
    start_date,
    status,
    end_date_weekofyear,
    cumulative_monthly_fee,
    monthly_fee_share,
    cumulative_monthly_fee_adjusted,
    first_known_cumulative_monthly_fee,
    last_known_end_date_weekofyear,
    CASE WHEN status RLIKE '^(PAUSED|EXPIRED)$' THEN 'PRIORITY' ELSE 'GROUP_B' END AS status_zone
FROM metrics_subscriptions
ORDER BY cumulative_monthly_fee DESC NULLS FIRST
