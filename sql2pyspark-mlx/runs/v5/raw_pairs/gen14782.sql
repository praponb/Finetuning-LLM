WITH filtered AS (
    -- Keep recent, valid rows
    SELECT
        id,
        name,
        email,
        country,
        city,
        segment,
        signup_date,
        credit_limit,
        age,
        CASE WHEN country IN ('SG', 'DE') THEN 'Core' ELSE 'Other' END country_group,
        TRIM(email) clean_email,
        COALESCE(segment, 'DIRECT') resolved_segment
    FROM customers
    WHERE segment IS NOT NULL
      AND city IS DISTINCT FROM 'Tokyo'
),
latest AS (
    -- Deduplicate with ROW_NUMBER
    SELECT
        id,
        name,
        country,
        city,
        segment,
        signup_date,
        credit_limit,
        age,
        country_group
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY country ORDER BY signup_date DESC NULLS FIRST, id) AS row_num
        FROM filtered
    ) t
    WHERE row_num = 1
),
ranked AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY segment, country ORDER BY signup_date DESC, id) AS dedup_rank
        FROM latest
    ) x
    WHERE dedup_rank = 1
),
with_last AS (
    -- Latest matching account per row (LATERAL ... LIMIT 1)
    SELECT
        s.id,
        s.country,
        s.segment,
        s.dedup_rank,
        x.account_type
    FROM ranked s
    JOIN LATERAL (
            SELECT c.account_type
            FROM accounts c
            WHERE c.customer_id = s.id
            ORDER BY c.balance DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
)
SELECT
    id,
    country,
    account_type,
    CASE WHEN account_type RLIKE '^(CREDIT|SAVINGS)$' THEN 'PRIORITY' ELSE 'OTHER_ZONE' END AS account_type_zone
FROM with_last
ORDER BY account_type DESC NULLS LAST;
