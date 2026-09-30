WITH latest_match AS (
    -- Latest matching account per row (LATERAL ... LIMIT 1)
    SELECT s.*, top1.account_account_type
    FROM customers s
    LEFT JOIN LATERAL (
            SELECT c.account_type AS account_account_type
            FROM accounts c
            WHERE c.customer_id = s.id
            ORDER BY c.opened_date DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
),
kept_keys AS (
    -- Set difference on keys
    SELECT city
    FROM latest_match
    EXCEPT
    SELECT city
    FROM customers
    WHERE segment IN ('Consumer', 'SMB')
),
kept_rows AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM latest_match s
    INNER JOIN kept_keys k ON s.city = k.city
),
matched_keys AS (
    -- Set difference on keys
    SELECT country, city
    FROM kept_rows
    EXCEPT ALL
    SELECT country, city
    FROM customers
    WHERE city = 'Berlin'
),
reconciled AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM kept_rows s
    INNER JOIN matched_keys k ON s.country = k.country AND s.city = k.city
),
scored AS (
    -- Running and moving aggregates
    SELECT
        country,
        city,
        signup_date,
        is_active,
        credit_limit,
        CUME_DIST() OVER w_hist AS age_cume_dist,
        CORR(credit_limit, age) OVER (PARTITION BY account_account_type, segment) AS corr_credit_limit_age,
        DENSE_RANK() OVER w_hist AS age_dense_rank
    FROM reconciled
    WINDOW w_hist AS (ORDER BY age DESC NULLS FIRST)
),
valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        country,
        signup_date,
        is_active,
        credit_limit,
        corr_credit_limit_age,
        CASE WHEN is_active THEN 'Yes' ELSE 'No' END AS is_active_label,
        LEAST(age_dense_rank, corr_credit_limit_age) AS least_age_dense_rank_corr_credit_limit_age
    FROM scored
    WHERE signup_date > DATE '2023-08-21'
      AND city IS NOT NULL
),
filtered AS (
    -- Basic cleansing and defaults
    SELECT *, CASE WHEN country = 'DE' THEN 1 ELSE 0 END AS is_de
    FROM valid
    WHERE signup_date > TO_DATE('2023-03-12')
),
base AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        country,
        is_active,
        corr_credit_limit_age,
        is_active_label,
        INITCAP(TRIM(country)) AS country_title
    FROM filtered
    WHERE signup_date > TO_DATE('2023-05-25')
)
-- Grouped metrics
SELECT
    country,
    ROUND(SUM(corr_credit_limit_age) / NULLIF(COUNT(CASE WHEN country = 'UK' THEN 1 END), 0), 2) AS corr_credit_limit_age_per_uk,
    COUNT(*) FILTER (WHERE country = 'UK') AS uk_rows,
    COUNT(CASE WHEN country = 'TH' THEN 1 END) AS th_count,
    COUNT(DISTINCT country) AS distinct_countries,
    SIZE(COLLECT_SET(country)) AS num_countries,
    ROUND(AVG(corr_credit_limit_age), 2) AS avg_corr_credit_limit_age
FROM base
GROUP BY CUBE (country)
ORDER BY distinct_countries DESC;
