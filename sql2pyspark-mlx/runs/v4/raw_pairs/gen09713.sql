WITH cleaned AS (
    -- Basic cleansing and defaults
    SELECT *, ADD_MONTHS(start_date, 12) AS start_date_plus_months
    FROM subscriptions
    WHERE end_date >= TO_DATE('2023-11-17')
      AND customer_id IS NOT NULL
),
enriched AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        customer_id,
        plan,
        start_date,
        end_date,
        monthly_fee,
        status,
        start_date_plus_months,
        ROUND(monthly_fee / SUM(monthly_fee) OVER (PARTITION BY customer_id, plan), 4) AS monthly_fee_share,
        RANK() OVER w_running AS monthly_fee_rank,
        CUME_DIST() OVER (ORDER BY monthly_fee NULLS FIRST) AS monthly_fee_cume_dist
    FROM cleaned
    WINDOW w_running AS (ORDER BY monthly_fee DESC NULLS FIRST)
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        customer_id,
        plan,
        start_date,
        end_date,
        monthly_fee,
        status,
        start_date_plus_months,
        monthly_fee_share,
        monthly_fee_rank,
        monthly_fee_cume_dist,
        LAG(monthly_fee_share, 1) OVER (ORDER BY start_date_plus_months, id) AS prev_monthly_fee_share,
        ROW_NUMBER() OVER (PARTITION BY plan ORDER BY end_date, id) AS event_seq,
        MIN(monthly_fee_rank) OVER (PARTITION BY customer_id) AS customer_id_min_monthly_fee_rank
    FROM enriched
),
latest AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY plan ORDER BY customer_id_min_monthly_fee_rank DESC, id) AS dedup_rank
        FROM metrics
    ) x
    WHERE dedup_rank = 1
),
enriched_subscriptions AS (
    -- Attach customers attributes
    SELECT
        a.*,
        d.signup_date,
        d.email,
        d.credit_limit
    FROM latest a
    LEFT JOIN customers d
        ON a.customer_id = d.id
),
with_dims AS (
    -- Attach customers attributes
    SELECT
        e.*,
        c.segment,
        c.email AS customer_email
    FROM enriched_subscriptions e
    JOIN customers c
        ON e.customer_id = c.id
)
-- Native SQL PIVOT
SELECT *
FROM (
    SELECT
        customer_id,
        segment,
        monthly_fee_cume_dist
    FROM with_dims
) src
PIVOT (
    AVG(monthly_fee_cume_dist) AS total,
    COUNT(1) AS n
    FOR segment IN ('SMB' AS smb, 'Consumer' AS consumer)
)
