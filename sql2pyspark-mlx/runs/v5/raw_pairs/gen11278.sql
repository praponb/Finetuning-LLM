WITH with_last AS (
    -- Latest matching session per row (LATERAL ... LIMIT 1)
    SELECT
        s.id,
        s.name,
        s.email,
        s.country,
        s.city,
        s.signup_date,
        s.is_active,
        s.age,
        top1.browser,
        top1.session_country
    FROM customers s
    LEFT JOIN LATERAL (
            SELECT c.browser, c.country AS session_country
            FROM sessions c
            WHERE c.customer_id = s.id
              AND c.started_at <= s.signup_date
            ORDER BY c.started_at DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
),
scored AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        SUM(age) OVER (PARTITION BY browser) AS browser_sum_age,
        ROW_NUMBER() OVER w_running AS seq_no,
        FIRST_VALUE(session_country) IGNORE NULLS OVER (PARTITION BY session_country ORDER BY signup_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_session_country,
        NTILE(5) OVER w_recent AS age_bucket
    FROM with_last
    WINDOW w_running AS (PARTITION BY city ORDER BY age, id), w_recent AS (PARTITION BY browser ORDER BY age DESC, id)
),
deduped AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *, ROW_NUMBER() OVER (PARTITION BY city ORDER BY age NULLS LAST, id) AS row_num
    FROM scored
    QUALIFY row_num = 1
),
latest_match AS (
    -- Latest matching account per row (LATERAL ... LIMIT 1)
    SELECT
        s.id,
        s.name,
        s.email,
        s.country,
        s.browser,
        s.session_country,
        s.browser_sum_age,
        s.first_known_session_country,
        top1.account_status,
        top1.balance
    FROM deduped s
    JOIN LATERAL (
            SELECT c.status AS account_status, c.balance
            FROM accounts c
            WHERE c.customer_id = s.id
            ORDER BY c.balance DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
),
grouped AS (
    -- Aggregate with conditional metrics
    SELECT
        browser,
        CASE WHEN browser_sum_age >= 249 THEN 'PREMIUM' ELSE 'STANDARD' END AS browser_sum_age_tier,
        SUM(balance) AS total_balance,
        ROUND(AVG(balance), 2) AS avg_balance,
        COUNT(*) AS num_rows
    FROM latest_match
    GROUP BY 1, 2
    HAVING num_rows >= 1
)
SELECT
    browser,
    browser_sum_age_tier,
    total_balance
FROM grouped
ORDER BY total_balance NULLS LAST;
