WITH base AS (
    SELECT *, COALESCE(segment, 'N/A') AS segment_filled
    FROM customers
    WHERE signup_date >= TO_DATE('2023-06-02')
      AND name LIKE 'K%'
),
matched_keys AS (
    SELECT id, segment
    FROM base
    INTERSECT
    SELECT id, segment
    FROM customers
    WHERE LOWER(name) LIKE '%vy %'
),
verified AS (
    SELECT s.*
    FROM base s
    JOIN matched_keys k ON s.id = k.id AND s.segment = k.segment
),
cleaned AS (
    SELECT
        *,
        CASE WHEN segment IN ('Corporate', 'SMB') THEN 'Group A' ELSE 'Other' END AS segment_group,
        INITCAP(TRIM(segment)) AS segment_title,
        COALESCE(city, 'UNKNOWN') AS city_filled
    FROM verified
    WHERE segment IS NOT NULL
      AND city IN ('Singapore', 'Bangkok', 'New York')
),
not_flagged AS (
    SELECT s.*
    FROM cleaned s
    WHERE EXISTS (
            SELECT 1
            FROM accounts c
            WHERE c.customer_id = s.id
              AND c.account_type IN ('SAVINGS', 'CREDIT', 'CHECKING')
        )
),
kept AS (
    SELECT s.*
    FROM not_flagged s
    WHERE NOT EXISTS (
            SELECT 1
            FROM sessions c
            WHERE c.customer_id = s.id
              AND c.started_at < s.signup_date
        )
)
SELECT
    name,
    email,
    country,
    segment,
    signup_date,
    credit_limit,
    age,
    segment_group,
    segment_title
FROM kept
