WITH latest AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY account_type ORDER BY opened_date DESC NULLS FIRST, id) AS rank_in_group
        FROM accounts
    ) ranked
    WHERE rank_in_group = 1
),
with_latest AS (
    -- Latest matching transaction per row (LATERAL ... LIMIT 1)
    SELECT
        s.*,
        x.txn_time,
        x.transaction_txn_type
    FROM latest s
    JOIN LATERAL (
            SELECT c.txn_time, c.txn_type AS transaction_txn_type
            FROM transactions c
            WHERE c.account_id = s.id
            ORDER BY c.txn_time DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
),
not_flagged AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM with_latest s
    WHERE NOT EXISTS (
            SELECT 1
            FROM transactions x
            WHERE x.account_id = s.id
              AND x.merchant IN ('Starbucks', 'Lazada')
        )
),
enriched AS (
    -- Attach customers attributes
    SELECT e.*, b.city
    FROM not_flagged e
    LEFT JOIN customers b
        ON e.customer_id = b.id
)
SELECT
    id,
    customer_id,
    balance,
    opened_date,
    rank_in_group,
    txn_time,
    transaction_txn_type,
    city,
    CASE WHEN city RLIKE '^(London|New York)$' THEN 'GROUP_B' WHEN city RLIKE '^(Bangkok|Chiang Mai)$' THEN 'OTHER_ZONE' ELSE 'CORE' END AS city_zone
FROM enriched;
