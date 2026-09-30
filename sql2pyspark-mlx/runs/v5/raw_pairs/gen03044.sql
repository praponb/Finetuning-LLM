WITH filtered AS (
    SELECT
        id,
        customer_id,
        account_type,
        balance,
        opened_date,
        status,
        CONCAT(account_type, '/', status) AS account_type_status
    FROM accounts
    WHERE customer_id IS NOT NULL
      AND opened_date >= '2023-11-18'
),
deduped AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY status ORDER BY balance DESC, id) AS dedup_rank
    FROM filtered
    QUALIFY dedup_rank = 1
),
with_latest AS (
    SELECT s.*, top1.merchant
    FROM deduped s
    JOIN LATERAL (
            SELECT c.merchant
            FROM transactions c
            WHERE c.account_id = s.id
            ORDER BY c.amount DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
),
enriched AS (
    SELECT
        id,
        account_type,
        balance,
        opened_date,
        account_type_status,
        dedup_rank,
        merchant,
        NTILE(4) OVER w_grp AS balance_bucket,
        FIRST_VALUE(merchant) IGNORE NULLS OVER (PARTITION BY customer_id ORDER BY opened_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_merchant,
        SUM(balance) OVER (PARTITION BY customer_id ORDER BY balance, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_balance,
        FIRST_VALUE(status) IGNORE NULLS OVER w_part AS first_known_status,
        ROW_NUMBER() OVER w_recent AS row_seq
    FROM with_latest
    WHERE opened_date BETWEEN '2023-06-01' AND '2025-07-01'
    WINDOW w_grp AS (PARTITION BY merchant ORDER BY balance DESC, id), w_part AS (PARTITION BY merchant, status ORDER BY dedup_rank, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), w_recent AS (PARTITION BY merchant ORDER BY dedup_rank, id)
),
eligible AS (
    SELECT s.*
    FROM enriched s
    WHERE NOT EXISTS (
            SELECT 1
            FROM transactions ch
            WHERE ch.account_id = s.id
              AND ch.account_id IN (20, 23, 38)
        )
),
cleaned AS (
    SELECT
        id,
        account_type,
        balance,
        opened_date,
        account_type_status,
        merchant,
        balance_bucket,
        first_known_merchant,
        running_balance,
        first_known_status,
        row_seq,
        FLOOR(balance) AS balance_whole
    FROM eligible
    WHERE opened_date > '2025-09-29'
),
base AS (
    SELECT
        *,
        FLOOR(row_seq) AS row_seq_whole,
        ABS(running_balance) AS abs_running_balance
    FROM cleaned
    WHERE opened_date >= DATE '2023-06-01'
      AND merchant IS NOT NULL
)
SELECT
    CASE WHEN balance >= 68000 THEN 'PREMIUM' ELSE 'STANDARD' END AS balance_tier,
    SORT_ARRAY(COLLECT_SET(account_type)) AS account_type_list,
    MAX(balance_bucket) AS max_balance_bucket
FROM base
GROUP BY ROLLUP (CASE WHEN balance >= 68000 THEN 'PREMIUM' ELSE 'STANDARD' END)
HAVING MAX(balance_bucket) < 1000000
