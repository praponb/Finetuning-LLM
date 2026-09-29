WITH joined AS (
    -- Attach accounts attributes
    SELECT e.*, b.balance
    FROM transactions e
    LEFT JOIN accounts b
        ON e.account_id = b.id
    WHERE b.account_type = 'SAVINGS'
),
enriched AS (
    -- Ranking and gap features
    SELECT
        id,
        account_id,
        txn_type,
        amount,
        txn_time,
        is_flagged,
        SUM(amount) OVER (PARTITION BY merchant ORDER BY balance, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_amount,
        LAG(amount, 2) OVER (ORDER BY txn_time, id) AS prev_amount,
        CUME_DIST() OVER (PARTITION BY account_id ORDER BY amount DESC NULLS LAST) AS amount_cume_dist
    FROM joined
),
filtered AS (
    -- Keep recent, valid rows
    SELECT
        id,
        account_id,
        amount,
        txn_time,
        is_flagged,
        running_amount,
        prev_amount,
        amount_cume_dist,
        IF(is_flagged, 'Yes', 'No') AS is_flagged_label
    FROM enriched
    WHERE NOT is_flagged
),
ranked AS (
    -- QUALIFY-style window filter
    SELECT *, ROW_NUMBER() OVER (PARTITION BY account_id ORDER BY txn_time DESC, id) AS rank_in_group
    FROM filtered
    QUALIFY rank_in_group = 1
)
-- Join a dimension and aggregate
SELECT
    d.account_type,
    COUNT(*) AS cnt,
    ROUND(SUM(src.amount), 2) AS total_amount
FROM ranked src
INNER JOIN accounts d ON src.account_id = d.id
GROUP BY d.account_type
ORDER BY total_amount DESC NULLS LAST
