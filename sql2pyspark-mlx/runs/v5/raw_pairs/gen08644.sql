WITH cleaned AS (
    SELECT
        id,
        customer_id,
        account_type,
        balance,
        opened_date,
        status,
        DATE_SUB(opened_date, 27) AS opened_date_minus_27d,
        INITCAP(TRIM(status)) AS status_title
    FROM accounts
    WHERE status IS NOT NULL
      AND opened_date >= DATE '2023-04-19'
),
enriched AS (
    SELECT
        a.id,
        a.customer_id,
        a.account_type,
        a.balance,
        a.status_title,
        d.email,
        d.country
    FROM cleaned a
    LEFT JOIN customers d
        ON a.customer_id <=> d.id
),
metrics AS (
    SELECT
        id,
        balance,
        email,
        country,
        LAG(balance) OVER (PARTITION BY country ORDER BY balance, id) AS prev_balance,
        LEAD(balance) OVER (PARTITION BY country ORDER BY balance, id) AS next_balance,
        ROW_NUMBER() OVER (PARTITION BY country ORDER BY balance, id) AS event_seq
    FROM enriched
),
filtered AS (
    SELECT
        *,
        SPLIT(LOWER(email), '@')[0] AS email_user,
        ROUND(prev_balance * 1.07, 2) AS adjusted_prev_balance,
        COALESCE(country, 'N/A') AS country_filled
    FROM metrics
    WHERE country IS NOT NULL
      AND prev_balance BETWEEN 36000 AND 38000
),
metrics_accounts AS (
    SELECT
        id,
        balance,
        email,
        country,
        event_seq,
        country_filled,
        LAST_VALUE(country) IGNORE NULLS OVER (PARTITION BY country ORDER BY adjusted_prev_balance, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_country,
        ROW_NUMBER() OVER (PARTITION BY country ORDER BY balance, id) AS row_seq,
        RANK() OVER (ORDER BY event_seq DESC) AS event_seq_rank,
        LAST_VALUE(next_balance) IGNORE NULLS OVER (PARTITION BY country ORDER BY next_balance, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_next_balance,
        LAG(next_balance, 1) OVER (PARTITION BY country ORDER BY balance, id) AS prev_next_balance
    FROM filtered
    WHERE balance BETWEEN 37000 AND 40000
)
SELECT
    country,
    PERCENTILE_APPROX(balance, ARRAY(0.25, 0.5, 0.75)) AS balance_quartiles,
    SUM(prev_next_balance) FILTER (WHERE country = 'US') AS us_total_prev_next_balance,
    COUNT(*) AS num_rows,
    COUNT(*) AS num_rows_2,
    COUNT(DISTINCT country) AS distinct_countries
FROM metrics_accounts
GROUP BY country
HAVING distinct_countries < 1000
ORDER BY num_rows
