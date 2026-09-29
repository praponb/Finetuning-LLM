WITH ranked AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM subscriptions
    QUALIFY RANK() OVER (PARTITION BY status ORDER BY monthly_fee DESC NULLS LAST) = 1
),
enriched AS (
    -- Running and moving aggregates
    SELECT
        id,
        customer_id,
        plan,
        start_date,
        end_date,
        monthly_fee,
        status,
        LAST_VALUE(monthly_fee, TRUE) OVER w_recent AS last_known_monthly_fee,
        LAG(monthly_fee, 1) OVER w_part AS prev_monthly_fee,
        LAST_VALUE(monthly_fee, TRUE) OVER (PARTITION BY status, plan ORDER BY start_date ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_monthly_fee_2
    FROM ranked
    WINDOW w_recent AS (PARTITION BY status ORDER BY end_date ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), w_part AS (ORDER BY end_date ASC, id ASC)
),
ranked_subscriptions AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY monthly_fee ASC NULLS FIRST, id ASC) AS rn
        FROM enriched
    ) ranked
    WHERE rn = 1
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        *,
        LAG(monthly_fee, 1) OVER (PARTITION BY status, plan ORDER BY start_date ASC, id ASC) AS prev_monthly_fee_2,
        ROUND(last_known_monthly_fee_2 / SUM(last_known_monthly_fee_2) OVER (PARTITION BY customer_id), 4) AS last_known_monthly_fee_2_share,
        CUME_DIST() OVER (PARTITION BY plan ORDER BY monthly_fee DESC NULLS LAST) AS monthly_fee_cume_dist
    FROM ranked_subscriptions
),
valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        customer_id,
        plan,
        start_date,
        end_date,
        last_known_monthly_fee,
        prev_monthly_fee,
        rn,
        prev_monthly_fee_2,
        last_known_monthly_fee_2_share,
        LEAST(last_known_monthly_fee, monthly_fee) AS least_last_known_monthly_fee_monthly_fee,
        CAST(monthly_fee_cume_dist AS INT) AS monthly_fee_cume_dist_int
    FROM with_windows
    WHERE end_date >= DATE '2023-05-18'
      AND last_known_monthly_fee < 110
)
SELECT
    id,
    customer_id,
    end_date,
    last_known_monthly_fee,
    prev_monthly_fee,
    rn,
    last_known_monthly_fee_2_share,
    least_last_known_monthly_fee_monthly_fee,
    monthly_fee_cume_dist_int
FROM valid
ORDER BY last_known_monthly_fee DESC NULLS FIRST
