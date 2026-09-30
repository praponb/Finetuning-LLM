WITH kept_keys AS (
    -- Set difference on keys
    SELECT status, id
    FROM accounts
    EXCEPT
    SELECT status, id
    FROM accounts
    WHERE customer_id = 17
),
verified AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM accounts s
    INNER JOIN kept_keys k ON s.status = k.status AND s.id = k.id
),
with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        STDDEV_SAMP(balance) OVER (PARTITION BY account_type) AS account_type_stddev_samp_balance,
        PERCENT_RANK() OVER w_recent AS balance_percent_rank,
        CUME_DIST() OVER (ORDER BY balance DESC NULLS FIRST) AS balance_cume_dist,
        MAX(balance) OVER w_ordered AS status_max_balance,
        MAX(balance) OVER w_ordered AS status_max_balance_2
    FROM verified
    WINDOW w_recent AS (PARTITION BY account_type ORDER BY balance DESC), w_ordered AS (PARTITION BY status, account_type)
),
enriched AS (
    -- Attach customers attributes
    SELECT
        s.*,
        p.city,
        p.segment
    FROM with_windows s
    LEFT JOIN customers p
        ON s.customer_id = p.id
),
kept AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM enriched s
    WHERE EXISTS (
            SELECT 1
            FROM transactions ch
            WHERE ch.account_id = s.id
              AND ch.txn_time < s.opened_date
        )
),
metrics AS (
    -- Running and moving aggregates
    SELECT *, STDDEV_SAMP(status_max_balance) OVER (PARTITION BY segment) AS segment_stddev_samp_status_max_balance
    FROM kept
)
-- Multi-level aggregation
SELECT
    CASE WHEN status_max_balance_2 >= 76461 THEN 'HIGH' ELSE 'LOW' END AS status_max_balance_2_tier,
    ROUND(SUM(balance_cume_dist) / NULLIF(COUNT(CASE WHEN city = 'New York' THEN 1 END), 0), 2) AS balance_cume_dist_per_new_york,
    SUM(status_max_balance_2) FILTER (WHERE status = 'FROZEN') AS frozen_total_status_max_balance_2,
    SUM(CASE WHEN account_type = 'LOAN' THEN segment_stddev_samp_status_max_balance ELSE 0 END) AS loan_segment_stddev_samp_status_max_balance,
    PERCENTILE_APPROX(segment_stddev_samp_status_max_balance, ARRAY(0.25, 0.5, 0.75)) AS segment_stddev_samp_status_max_balance_quartiles
FROM metrics
GROUP BY 1;
