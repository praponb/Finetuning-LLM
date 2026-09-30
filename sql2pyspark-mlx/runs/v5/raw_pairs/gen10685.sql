WITH valid AS (
    -- Keep recent, valid rows
    SELECT
        id,
        customer_id,
        plan,
        end_date,
        monthly_fee,
        status,
        DATEDIFF(start_date, end_date) AS days_end_date_to_start_date
    FROM subscriptions
    WHERE customer_id IS NOT NULL
      AND status IN ('CANCELLED', 'ACTIVE', 'EXPIRED')
),
enriched AS (
    -- Ranking and gap features
    SELECT
        id,
        customer_id,
        plan,
        status,
        LAST_VALUE(monthly_fee) IGNORE NULLS OVER (PARTITION BY status ORDER BY monthly_fee, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_monthly_fee,
        AVG(monthly_fee) OVER w_recent AS monthly_fee_avg_3
    FROM valid
    WINDOW w_recent AS (PARTITION BY plan ORDER BY days_end_date_to_start_date, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW)
),
deduped AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY last_known_monthly_fee DESC NULLS FIRST, id) AS latest_rank
        FROM enriched
    ) ranked
    WHERE latest_rank = 1
),
ranked AS (
    -- QUALIFY-style window filter
    SELECT
        id,
        plan,
        status,
        monthly_fee_avg_3,
        latest_rank
    FROM deduped
    QUALIFY ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY last_known_monthly_fee DESC, id) = 1
),
deduped_subscriptions AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY plan ORDER BY monthly_fee_avg_3 DESC NULLS LAST, id) AS latest_rank_2
        FROM ranked
    ) ranked
    WHERE latest_rank_2 = 1
),
cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        plan,
        status,
        monthly_fee_avg_3,
        latest_rank,
        ROUND(monthly_fee_avg_3, 0) AS monthly_fee_avg_3_rounded,
        CONCAT(status, ', ', plan) AS status_plan
    FROM deduped_subscriptions
    WHERE monthly_fee_avg_3 = 175
)
-- Grouped metrics
SELECT
    plan,
    APPROX_PERCENTILE(monthly_fee_avg_3_rounded, 0.95) AS monthly_fee_avg_3_rounded_p95,
    PERCENTILE_APPROX(monthly_fee_avg_3, 0.5) AS monthly_fee_avg_3_p50
FROM cleaned
GROUP BY plan;
