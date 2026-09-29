WITH with_latest AS (
    -- Latest matching transaction per row (LATERAL ... LIMIT 1)
    SELECT s.*, top1.amount
    FROM accounts s
    LEFT JOIN LATERAL (
            SELECT c.amount
            FROM transactions c
            WHERE c.account_id = s.id
            ORDER BY c.amount DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
),
metrics AS (
    -- Ranking and gap features
    SELECT
        id,
        customer_id,
        account_type,
        balance,
        opened_date,
        status,
        amount,
        ROUND(amount / SUM(amount) OVER (PARTITION BY account_type), 4) amount_share,
        MIN(balance) OVER (PARTITION BY account_type) account_type_min_balance,
        CUME_DIST() OVER (PARTITION BY customer_id, status ORDER BY amount NULLS LAST) amount_cume_dist,
        AVG(amount) OVER (PARTITION BY account_type ORDER BY balance, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) amount_avg_2_prior,
        ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY opened_date, id) event_seq
    FROM with_latest
),
enriched AS (
    -- Attach customers attributes
    SELECT e.*, d.email
    FROM metrics e
    LEFT JOIN customers d
        ON e.customer_id = d.id
),
kept AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM enriched s
    WHERE NOT EXISTS (
            SELECT 1
            FROM transactions c
            WHERE c.account_id = s.id
              AND HOUR(c.txn_time) BETWEEN 10 AND 18
        )
),
enriched_accounts AS (
    -- Attach customers attributes
    SELECT
        e.*,
        p.signup_date,
        p.email AS customer_email,
        p.name
    FROM kept e
    INNER JOIN customers p
        ON e.customer_id = p.id
),
ranked AS (
    -- QUALIFY-style window filter
    SELECT *, RANK() OVER (PARTITION BY status, account_type ORDER BY event_seq NULLS LAST) AS latest_rank
    FROM enriched_accounts
    QUALIFY latest_rank = 1
),
latest_match AS (
    -- Latest matching transaction per row (LATERAL ... LIMIT 1)
    SELECT s.*, top1.transaction_merchant
    FROM ranked s
    LEFT JOIN LATERAL (
            SELECT c.merchant AS transaction_merchant
            FROM transactions c
            WHERE c.account_id = s.id
            ORDER BY c.txn_time DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
)
SELECT
    s.id,
    s.customer_id,
    s.account_type,
    s.balance,
    s.opened_date,
    s.status,
    s.account_type_min_balance,
    s.amount_cume_dist,
    s.amount_avg_2_prior,
    CASE WHEN status RLIKE '^(CLOSED|FROZEN)$' THEN 'CORE' WHEN status RLIKE '^(OPEN)$' THEN 'GROUP_B' ELSE 'OTHER_ZONE' END AS status_zone,
    (SELECT MIN(x.discount) FROM orders x WHERE x.customer_id = s.customer_id) AS min_order_discount
FROM latest_match s
ORDER BY status_zone NULLS FIRST;
