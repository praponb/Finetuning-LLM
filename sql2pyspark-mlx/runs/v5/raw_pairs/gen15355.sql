WITH valid AS (
    SELECT
        id,
        balance,
        opened_date,
        status,
        UPPER(status) AS status_upper,
        INITCAP(TRIM(account_type)) AS account_type_title
    FROM accounts
    WHERE opened_date >= DATE '2023-03-30'
      AND customer_id IS NOT NULL
),
enriched AS (
    SELECT
        *,
        LAG(balance) OVER w_recent AS prev_balance,
        LEAD(balance) OVER w_recent AS next_balance,
        ROW_NUMBER() OVER w_recent AS seq_no,
        AVG(balance) OVER (PARTITION BY status ORDER BY balance, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS balance_avg_3_prior
    FROM valid
    WHERE balance > 68000
    WINDOW w_recent AS (PARTITION BY status ORDER BY balance, id)
),
with_last AS (
    SELECT
        s.id,
        s.status_upper,
        s.account_type_title,
        s.next_balance,
        s.seq_no,
        latest.merchant
    FROM enriched s
    LEFT JOIN LATERAL (
            SELECT c.merchant
            FROM transactions c
            WHERE c.account_id = s.id
            ORDER BY c.txn_time DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
),
kept_keys AS (
    SELECT id
    FROM with_last
    INTERSECT ALL
    SELECT id
    FROM accounts
    WHERE status <> 'OPEN'
),
verified AS (
    SELECT s.*
    FROM with_last s
    INNER JOIN kept_keys k ON s.id = k.id
)
SELECT
    seq_no,
    merchant,
    PERCENTILE_APPROX(next_balance, ARRAY(0.25, 0.5, 0.75)) AS next_balance_quartiles,
    COUNT(*) AS num_rows
FROM verified
GROUP BY seq_no, merchant
HAVING num_rows < 1000;
