WITH cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        name,
        email,
        country,
        city,
        segment,
        signup_date,
        is_active,
        credit_limit,
        CONCAT(city, ', ', name) AS city_name,
        COALESCE(segment, 'DIRECT') AS segment_clean
    FROM customers
    WHERE signup_date >= TO_DATE('2023-04-15')
      AND segment IS NOT NULL
),
ranked AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY country ORDER BY credit_limit DESC, id) AS rank_in_group
        FROM cleaned
    ) ranked
    WHERE rank_in_group = 1
),
deduped AS (
    -- QUALIFY-style window filter
    SELECT *, ROW_NUMBER() OVER (PARTITION BY city, country ORDER BY credit_limit NULLS LAST, id) AS row_num
    FROM ranked
    QUALIFY row_num = 1
),
latest AS (
    -- Keep one row per key (latest first)
    SELECT *, ROW_NUMBER() OVER (PARTITION BY country ORDER BY signup_date, id) AS rank_in_group_2
    FROM deduped
    QUALIFY rank_in_group_2 = 1
),
kept_keys AS (
    -- Set intersection on keys
    SELECT segment
    FROM latest
    INTERSECT
    SELECT segment
    FROM customers
    WHERE country = 'DE'
),
reconciled AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM latest s
    INNER JOIN kept_keys k ON s.segment = k.segment
),
remaining_keys AS (
    -- Set intersection on keys
    SELECT id, segment
    FROM reconciled
    INTERSECT ALL
    SELECT id, segment
    FROM customers
    WHERE YEAR(signup_date) = 2025
),
kept_rows AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM reconciled s
    INNER JOIN remaining_keys k ON s.id = k.id AND s.segment = k.segment
),
grouped AS (
    -- Multi-level aggregation
    SELECT
        CASE WHEN credit_limit >= 14600 THEN 'HIGH' ELSE 'LOW' END AS credit_limit_tier,
        COUNT_IF(city = 'Bangkok') AS bangkok_events,
        PERCENTILE_APPROX(credit_limit, ARRAY(0.25, 0.5, 0.75)) AS credit_limit_quartiles,
        ROUND(SUM(credit_limit) / NULLIF(COUNT(CASE WHEN segment = 'SMB' THEN 1 END), 0), 2) AS credit_limit_per_smb
    FROM kept_rows
    GROUP BY credit_limit_tier
    HAVING ROUND(SUM(credit_limit) / NULLIF(COUNT(CASE WHEN segment = 'SMB' THEN 1 END), 0), 2) > 0
)
SELECT
    credit_limit_tier,
    bangkok_events,
    credit_limit_quartiles
FROM grouped
ORDER BY bangkok_events NULLS LAST;
