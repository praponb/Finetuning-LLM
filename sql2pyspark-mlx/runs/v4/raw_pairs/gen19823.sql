WITH filtered AS (
    -- Keep recent, valid rows
    SELECT
        id,
        store_id,
        sale_date,
        units,
        revenue,
        FLOOR(revenue) AS revenue_whole,
        LEAST(units, revenue) AS least_units_revenue
    FROM store_sales
    WHERE sale_date > DATE '2023-08-11'
      AND units <= 36
),
cleaned AS (
    -- Keep recent, valid rows
    SELECT
        id,
        store_id,
        units,
        revenue,
        revenue_whole,
        least_units_revenue,
        LAST_DAY(sale_date) AS sale_date_month_end,
        ROUND(revenue_whole, 0) AS revenue_whole_rounded
    FROM filtered
    WHERE sale_date > TO_DATE('2023-08-23')
      AND revenue_whole IS NULL
),
ranked AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY store_id ORDER BY revenue_whole_rounded DESC NULLS LAST, id) AS dedup_rank
        FROM cleaned
    ) t
    WHERE dedup_rank = 1
),
totals AS (
    -- Multi-level aggregation
    SELECT
        DATE_TRUNC('QUARTER', sale_date_month_end) AS quarter_start,
        SUM(least_units_revenue) AS total_least_units_revenue,
        BIT_AND(units) AS units_bit_and,
        COUNT(DISTINCT id) AS distinct_ids,
        COUNT(DISTINCT store_id) AS distinct_store_ids,
        COUNT(*) AS cnt
    FROM ranked
    GROUP BY DATE_TRUNC('QUARTER', sale_date_month_end)
)
SELECT
    quarter_start,
    total_least_units_revenue,
    units_bit_and,
    distinct_ids
FROM totals;
