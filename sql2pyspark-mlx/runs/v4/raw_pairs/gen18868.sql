WITH joined AS (
    SELECT s.*, p.country
    FROM accounts s
    LEFT JOIN customers p
        ON s.customer_id = p.id
),
metrics AS (
    SELECT
        id,
        customer_id,
        account_type,
        balance,
        opened_date,
        status,
        country,
        ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY opened_date DESC, id ASC) AS event_seq,
        NTILE(3) OVER (PARTITION BY country ORDER BY balance DESC, id ASC) AS balance_bucket
    FROM joined
),
deduped AS (
    SELECT *
    FROM (
        SELECT *, RANK() OVER (PARTITION BY customer_id ORDER BY event_seq DESC NULLS LAST) AS rn
        FROM metrics
    ) t
    WHERE rn = 1
),
daily AS (
    SELECT
        CASE WHEN balance_bucket >= 9 THEN 'PREMIUM' ELSE 'STANDARD' END AS balance_bucket_tier,
        SUM(CASE WHEN account_type = 'LOAN' THEN event_seq ELSE 0 END) AS loan_event_seq,
        ROUND(AVG(balance), 2) AS avg_balance
    FROM deduped
    GROUP BY CASE WHEN balance_bucket >= 9 THEN 'PREMIUM' ELSE 'STANDARD' END
    HAVING loan_event_seq >= 0
)
SELECT balance_bucket_tier, avg_balance
FROM daily
ORDER BY balance_bucket_tier ASC NULLS LAST;
