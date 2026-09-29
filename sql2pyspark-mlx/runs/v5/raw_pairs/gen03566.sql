WITH deduped AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM (
        SELECT *, RANK() OVER (PARTITION BY customer_id ORDER BY opened_date DESC) AS rank_in_group
        FROM accounts
    ) x
    WHERE rank_in_group = 1
),
latest AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM deduped
    QUALIFY ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY balance NULLS FIRST, id) = 1
),
scored AS (
    -- Ranking and gap features
    SELECT
        id,
        customer_id,
        account_type,
        opened_date,
        rank_in_group,
        ROW_NUMBER() OVER (ORDER BY rank_in_group, id) AS event_seq,
        AVG(balance) OVER (PARTITION BY status ORDER BY balance, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS balance_avg_2,
        PERCENT_RANK() OVER (PARTITION BY status, customer_id ORDER BY balance) AS balance_percent_rank
    FROM latest
),
with_latest AS (
    -- Latest matching transaction per row (LATERAL ... LIMIT 1)
    SELECT
        s.id,
        s.customer_id,
        s.account_type,
        s.balance_avg_2,
        s.balance_percent_rank,
        latest.transaction_amount
    FROM scored s
    LEFT JOIN LATERAL (
            SELECT c.amount AS transaction_amount
            FROM transactions c
            WHERE c.account_id = s.id
            ORDER BY c.amount DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
)
SELECT
    id,
    customer_id,
    balance_percent_rank,
    transaction_amount
FROM with_latest
ORDER BY id DESC, transaction_amount
