WITH filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        store_name,
        region,
        opened_date,
        sqft,
        DATEDIFF(CURRENT_DATE(), opened_date) days_since_opened_date
    FROM stores
    WHERE opened_date > TO_DATE('2023-08-19')
      AND region IS NOT NULL
),
ranked AS (
    -- Keep one row per key (latest first)
    SELECT
        id,
        store_name,
        opened_date
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY region ORDER BY opened_date ASC NULLS LAST, id ASC) AS latest_rank
        FROM filtered
    ) x
    WHERE latest_rank = 1
),
cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        DATE_FORMAT(opened_date, 'yyyy-MM-dd') opened_date_label,
        DATE_FORMAT(opened_date, 'yyyy-MM') opened_date_label_2
    FROM ranked
    WHERE opened_date >= DATE '2023-08-09'
),
base AS (
    -- Basic cleansing and defaults
    SELECT *, LAST_DAY(opened_date) opened_date_month_end
    FROM cleaned
    WHERE opened_date BETWEEN '2024-04-01' AND '2025-05-01'
)
-- Grouped metrics
SELECT
    DATE_TRUNC('QUARTER', opened_date) AS quarter_start,
    COUNT(*) AS num_rows,
    COUNT(DISTINCT id) AS distinct_ids,
    COUNT(DISTINCT id) AS distinct_ids_2
FROM base
GROUP BY 1
HAVING COUNT(DISTINCT id) > 1
ORDER BY num_rows DESC;
