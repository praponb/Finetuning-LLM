WITH scored AS (
    -- Running and moving aggregates
    SELECT
        id,
        email,
        country,
        segment,
        LEAD(age) OVER (PARTITION BY segment ORDER BY credit_limit, id) AS next_age,
        LAG(age) OVER (PARTITION BY segment ORDER BY credit_limit, id) AS prev_age,
        LAG(credit_limit) OVER (PARTITION BY segment ORDER BY credit_limit, id) AS prev_credit_limit,
        ROW_NUMBER() OVER (PARTITION BY segment ORDER BY credit_limit, id) AS event_seq
    FROM customers
    WHERE country IN ('TH', 'UK', 'JP')
),
cleaned AS (
    -- Keep recent, valid rows
    SELECT
        *,
        TRIM(email) AS clean_email,
        event_seq + next_age AS event_seq_plus_next_age
    FROM scored
    WHERE segment IS DISTINCT FROM 'Consumer'
      AND country = 'TH'
),
valid AS (
    -- Keep recent, valid rows
    SELECT
        id,
        email,
        country,
        segment,
        prev_credit_limit,
        event_seq,
        clean_email,
        event_seq_plus_next_age,
        event_seq * prev_credit_limit AS event_seq_times_prev_credit_limit,
        COALESCE(segment, 'OTHER') AS segment_clean
    FROM cleaned
    WHERE country IN ('US', 'TH')
),
with_latest AS (
    -- Latest matching account per row (LATERAL ... LIMIT 1)
    SELECT
        s.id,
        s.country,
        s.event_seq,
        top1.account_type,
        top1.balance
    FROM valid s
    LEFT JOIN LATERAL (
            SELECT c.account_type, c.balance
            FROM accounts c
            WHERE c.customer_id = s.id
            ORDER BY c.balance DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
),
matched_keys AS (
    -- Set difference on keys
    SELECT id, country
    FROM with_latest
    EXCEPT DISTINCT
    SELECT id, country
    FROM customers
    WHERE city IN ('Bangkok', 'Singapore')
),
kept_rows AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM with_latest s
    JOIN matched_keys k ON s.id = k.id AND s.country = k.country
),
with_windows AS (
    -- Window metrics over several different partitions
    SELECT *, MIN(balance) OVER (PARTITION BY country) AS country_min_balance
    FROM kept_rows
)
SELECT
    id,
    country,
    balance,
    country_min_balance,
    CASE WHEN country RLIKE '^(US|JP)$' THEN 'CORE' WHEN country RLIKE '^(TH|SG)$' THEN 'GROUP_B' ELSE 'EXTENDED' END AS country_zone
FROM with_windows
ORDER BY country_min_balance DESC, id
