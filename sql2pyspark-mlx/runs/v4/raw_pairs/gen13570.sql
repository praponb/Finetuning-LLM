WITH base AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        INITCAP(TRIM(status)) AS status_title,
        DATE_TRUNC('month', end_date) AS end_date_period
    FROM subscriptions
    WHERE start_date >= DATE '2023-10-01'
),
flagged AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN LAG(end_date_period) OVER (PARTITION BY plan ORDER BY end_date_period, id) IS NULL
                 OR end_date_period > LAG(end_date_period) OVER (PARTITION BY plan ORDER BY end_date_period, id) + INTERVAL 7 DAYS THEN 1 ELSE 0 END AS is_new_burst
    FROM base
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_burst) OVER (PARTITION BY plan ORDER BY end_date_period, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no
    FROM flagged
),
remaining_keys AS (
    -- Set intersection on keys
    SELECT plan, id
    FROM sessionized
    INTERSECT ALL
    SELECT plan, id
    FROM subscriptions
    WHERE end_date <= '2024-08-01'
),
reconciled AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM sessionized s
    INNER JOIN remaining_keys k ON s.plan = k.plan AND s.id = k.id
),
joined AS (
    -- Attach customers attributes
    SELECT
        a.*,
        p.credit_limit,
        p.name
    FROM reconciled a
    LEFT JOIN customers p
        ON a.customer_id = p.id
),
with_dims AS (
    -- Attach customers attributes
    SELECT
        a.*,
        c.age,
        c.credit_limit AS customer_credit_limit,
        c.name AS customer_name
    FROM joined a
    INNER JOIN customers c
        ON a.customer_id = c.id
),
base_subscriptions AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        customer_id,
        plan,
        start_date,
        monthly_fee,
        status,
        status_title,
        is_new_burst,
        session_no,
        credit_limit,
        age,
        customer_credit_limit,
        customer_name,
        credit_limit - is_new_burst AS credit_limit_minus_is_new_burst,
        DATE_FORMAT(end_date, 'yyyy-MM') AS end_date_label
    FROM with_dims
    WHERE end_date BETWEEN '2024-09-27' AND '2025-08-01'
),
with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        CUME_DIST() OVER (ORDER BY session_no DESC) AS session_no_cume_dist,
        PERCENT_RANK() OVER (ORDER BY session_no DESC) AS session_no_percent_rank,
        DENSE_RANK() OVER (ORDER BY is_new_burst NULLS FIRST) AS is_new_burst_dense_rank
    FROM base_subscriptions
)
SELECT
    id,
    customer_id,
    start_date,
    monthly_fee,
    status_title,
    age,
    customer_credit_limit,
    session_no_cume_dist
FROM with_windows
