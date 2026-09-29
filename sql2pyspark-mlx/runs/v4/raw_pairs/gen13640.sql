WITH cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        store_name,
        city,
        opened_date,
        DATEDIFF(CURRENT_DATE(), opened_date) AS days_since_opened_date
    FROM stores
    WHERE region IS NOT NULL
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        id,
        store_name,
        city,
        opened_date,
        days_since_opened_date,
        ROW_NUMBER() OVER (ORDER BY opened_date ASC, id ASC) AS row_seq,
        LEAD(days_since_opened_date, 1) OVER (ORDER BY opened_date ASC, id ASC) AS next_days_since_opened_date
    FROM cleaned
),
latest AS (
    -- QUALIFY-style window filter
    SELECT *, ROW_NUMBER() OVER (PARTITION BY city ORDER BY opened_date DESC NULLS LAST, id ASC) AS latest_rank
    FROM with_windows
    QUALIFY latest_rank = 1
)
SELECT
    id,
    store_name,
    opened_date,
    next_days_since_opened_date,
    latest_rank,
    CASE WHEN store_name RLIKE '^(Mall Central|Old Town)$' THEN 'PRIORITY' ELSE 'EXTENDED' END AS store_name_zone
FROM latest
ORDER BY next_days_since_opened_date DESC NULLS FIRST;
