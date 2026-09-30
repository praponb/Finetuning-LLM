WITH valid AS (
    -- Basic cleansing and defaults
    SELECT *, ROUND(balance * 1.07, 2) AS balance_scaled
    FROM accounts
    WHERE status IS NOT NULL
      AND status IS DISTINCT FROM 'OPEN'
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        *,
        CUME_DIST() OVER w_part AS balance_cume_dist,
        LAST_VALUE(balance, TRUE) OVER (PARTITION BY status ORDER BY balance ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_balance
    FROM valid
    WHERE status = 'OPEN'
    WINDOW w_part AS (PARTITION BY status ORDER BY balance DESC NULLS LAST)
),
enriched AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        customer_id,
        account_type,
        status,
        balance_scaled,
        balance_cume_dist,
        FIRST_VALUE(status, TRUE) OVER (ORDER BY balance_cume_dist ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_status,
        FIRST_VALUE(balance, TRUE) OVER (PARTITION BY account_type ORDER BY balance_cume_dist ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_balance
    FROM metrics
),
enriched_accounts AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        LEAD(balance_scaled) OVER (PARTITION BY account_type, status ORDER BY balance_cume_dist ASC, id ASC) AS next_balance_scaled,
        LAG(balance_scaled) OVER (PARTITION BY account_type, status ORDER BY balance_cume_dist ASC, id ASC) AS prev_balance_scaled,
        LEAD(first_known_balance) OVER (PARTITION BY account_type, status ORDER BY balance_cume_dist ASC, id ASC) AS next_first_known_balance,
        LAG(first_known_balance) OVER (PARTITION BY account_type, status ORDER BY balance_cume_dist ASC, id ASC) AS prev_first_known_balance,
        ROW_NUMBER() OVER (PARTITION BY account_type, status ORDER BY balance_cume_dist ASC, id ASC) AS row_seq
    FROM enriched
),
eligible AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM enriched_accounts s
    WHERE NOT EXISTS (
            SELECT 1
            FROM transactions c
            WHERE c.account_id = s.id
              AND c.is_flagged
        )
),
scored AS (
    -- Ranking and gap features
    SELECT
        id,
        customer_id,
        status,
        balance_scaled,
        balance_cume_dist,
        first_known_status,
        first_known_balance,
        prev_balance_scaled,
        prev_first_known_balance,
        SUM(prev_balance_scaled) OVER (PARTITION BY account_type ORDER BY prev_first_known_balance ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_prev_balance_scaled,
        SUM(next_balance_scaled) OVER (PARTITION BY account_type, customer_id ORDER BY next_balance_scaled ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_next_balance_scaled,
        FIRST_VALUE(prev_balance_scaled, TRUE) OVER (PARTITION BY account_type, customer_id ORDER BY first_known_balance ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_prev_balance_scaled
    FROM eligible
)
-- Join a dimension and aggregate
SELECT
    c.city,
    c.segment,
    a.status,
    COUNT(*) AS num_rows,
    MIN(a.first_known_balance) AS min_first_known_balance,
    GROUPING_ID() AS gid
FROM scored a
LEFT JOIN customers c ON a.customer_id = c.id
GROUP BY GROUPING SETS ((c.city, c.segment, a.status), (c.city), ())
