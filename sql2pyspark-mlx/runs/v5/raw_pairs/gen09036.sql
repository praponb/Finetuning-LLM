WITH ranked AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM (
        SELECT *, RANK() OVER (PARTITION BY plan ORDER BY end_date DESC NULLS FIRST) AS row_num
        FROM subscriptions
    ) x
    WHERE row_num = 1
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        id,
        customer_id,
        start_date,
        monthly_fee,
        NTILE(4) OVER w_grp AS monthly_fee_bucket,
        ROUND(monthly_fee / SUM(monthly_fee) OVER (PARTITION BY status), 4) AS monthly_fee_share,
        CUME_DIST() OVER (PARTITION BY status ORDER BY monthly_fee NULLS LAST) AS monthly_fee_cume_dist
    FROM ranked
    WINDOW w_grp AS (ORDER BY monthly_fee DESC, id)
),
kept_keys AS (
    -- Set difference on keys
    SELECT customer_id, id
    FROM with_windows
    EXCEPT ALL
    SELECT customer_id, id
    FROM subscriptions
    WHERE status IN ('ACTIVE', 'CANCELLED', 'PAUSED')
),
kept_rows AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM with_windows s
    INNER JOIN kept_keys k ON s.customer_id = k.customer_id AND s.id = k.id
),
joined AS (
    -- Attach customers attributes
    SELECT
        s.customer_id,
        s.start_date,
        s.monthly_fee,
        s.monthly_fee_share,
        s.monthly_fee_cume_dist,
        p.city,
        p.age
    FROM kept_rows s
    LEFT JOIN customers p
        ON s.customer_id = p.id
    WHERE p.city IS NULL
)
SELECT
    s.customer_id,
    s.monthly_fee,
    s.monthly_fee_cume_dist,
    (SELECT AVG(t2.balance) FROM accounts t2 WHERE t2.customer_id = s.customer_id) AS avg_account_balance
FROM joined s
ORDER BY avg_account_balance DESC;
