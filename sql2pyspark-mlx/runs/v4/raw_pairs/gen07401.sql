WITH valid AS (
    -- Keep recent, valid rows
    SELECT
        id,
        store_id,
        revenue,
        DATEDIFF(CURRENT_DATE(), sale_date) AS days_since_sale_date
    FROM store_sales
    WHERE sale_date > TO_DATE('2023-06-04')
),
enriched AS (
    -- Attach stores attributes
    SELECT
        a.*,
        b.city,
        b.region
    FROM valid a
    INNER JOIN stores b
        ON a.store_id = b.id
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        id,
        store_id,
        revenue,
        days_since_sale_date,
        city,
        region,
        SUM(days_since_sale_date) OVER (PARTITION BY store_id, city ORDER BY days_since_sale_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_days_since_sale_date,
        AVG(days_since_sale_date) OVER (PARTITION BY region ORDER BY days_since_sale_date, id ROWS BETWEEN 5 PRECEDING AND CURRENT ROW) AS days_since_sale_date_avg_5
    FROM enriched
)
-- Native SQL PIVOT
SELECT *
FROM (
    SELECT
        city,
        region,
        revenue
    FROM with_windows
) src
PIVOT (
    AVG(revenue) AS total,
    COUNT(1) AS cnt
    FOR region IN ('East', 'Central', 'North')
);
