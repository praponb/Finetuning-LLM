WITH flattened AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        created_at,
        attr_key,
        value
    FROM carts
    LATERAL VIEW OUTER EXPLODE(attributes) li AS attr_key, value
),
filtered AS (
    -- Keep recent, valid rows
    SELECT *, SUBSTRING(attr_key, 1, 2) AS attr_key_prefix
    FROM flattened
    WHERE created_at > DATE '2025-03-01'
      AND attr_key IS NOT NULL
),
filtered_carts AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        CASE WHEN attr_key IN ('gift', 'device') THEN 'Priority' ELSE 'Other' END AS attr_key_group,
        DATEDIFF(CURRENT_DATE(), created_at) AS days_since_created_at,
        COALESCE(attr_key, 'N/A') AS attr_key_clean
    FROM filtered
    WHERE created_at >= TO_TIMESTAMP('2025-03-01 16:00:00')
),
scored AS (
    -- Ranking and gap features
    SELECT
        *,
        COUNT(days_since_created_at) OVER (ORDER BY CAST(created_at AS BIGINT) RANGE BETWEEN 7200 PRECEDING AND CURRENT ROW) AS days_since_created_at_count_2h,
        SUM(days_since_created_at) OVER (PARTITION BY attr_key ORDER BY CAST(created_at AS BIGINT) RANGE BETWEEN 7200 PRECEDING AND CURRENT ROW) AS days_since_created_at_sum_2h
    FROM filtered_carts
    WHERE days_since_created_at = 25
),
cleaned AS (
    -- Keep recent, valid rows
    SELECT
        created_at,
        value,
        days_since_created_at,
        days_since_created_at_count_2h,
        DAYOFWEEK(created_at) AS created_at_dayofweek,
        COALESCE(attr_key, 'N/A') AS resolved_attr_key
    FROM scored
    WHERE created_at >= TO_DATE('2025-03-02')
)
SELECT
    created_at,
    value,
    days_since_created_at,
    created_at_dayofweek
FROM cleaned
