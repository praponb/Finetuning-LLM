WITH base AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        name,
        email,
        country,
        city,
        segment,
        signup_date,
        is_active,
        age,
        COALESCE(credit_limit, 0) credit_limit_filled,
        IF(is_active, 'Yes', 'No') is_active_label
    FROM customers
    WHERE segment IS NOT NULL
),
eligible AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM base s
    WHERE NOT EXISTS (
            SELECT 1
            FROM subscriptions ch
            WHERE ch.customer_id = s.id
              AND ch.end_date < s.signup_date
        )
),
not_flagged AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM eligible s
    WHERE NOT EXISTS (
            SELECT 1
            FROM orders ch
            WHERE ch.customer_id = s.id
              AND ch.order_date <= s.signup_date
        )
),
eligible_customers AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM not_flagged s
    WHERE NOT EXISTS (
            SELECT 1
            FROM subscriptions c
            WHERE c.customer_id = s.id
              AND c.start_date >= s.signup_date
        )
),
latest AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY segment ORDER BY credit_limit_filled DESC, id) AS dedup_rank
        FROM eligible_customers
    ) ranked
    WHERE dedup_rank = 1
),
pivoted AS (
    -- Native SQL PIVOT
    SELECT *
    FROM (
        SELECT
            country,
            segment,
            credit_limit_filled
        FROM latest
    ) src
    PIVOT (
        SUM(credit_limit_filled) AS amount,
        COUNT(1) AS cnt
        FOR segment IN ('Corporate' AS corporate, 'Consumer' AS consumer, 'SMB' AS smb)
    )
)
SELECT
    country,
    corporate_amount,
    corporate_cnt,
    consumer_cnt,
    smb_cnt
FROM pivoted
ORDER BY consumer_cnt DESC, corporate_amount DESC
