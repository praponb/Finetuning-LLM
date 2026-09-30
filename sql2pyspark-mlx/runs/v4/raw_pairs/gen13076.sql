WITH joined AS (
    -- Attach stores attributes
    SELECT
        src.*,
        p.opened_date,
        p.sqft,
        p.region
    FROM store_sales src
    INNER JOIN stores p
        ON src.store_id = p.id
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        *,
        AVG(revenue) OVER (ORDER BY opened_date, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS revenue_avg_2_prior,
        PERCENT_RANK() OVER (PARTITION BY store_id, product_id ORDER BY revenue NULLS LAST) AS revenue_percent_rank,
        SUM(units) OVER (PARTITION BY product_id, region ORDER BY sale_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_units
    FROM joined
),
joined_store_sales AS (
    -- Attach stores attributes
    SELECT
        a.*,
        b.store_name,
        b.city
    FROM with_windows a
    LEFT JOIN stores b
        ON a.store_id = b.id
)
-- Grouped metrics
SELECT
    DATE_TRUNC('MONTH', sale_date) AS activity_month,
    DATE_TRUNC('WEEK', sale_date) AS week_start,
    AVG(units) AS avg_units,
    ROUND(AVG(revenue_percent_rank), 2) AS avg_revenue_percent_rank,
    MAX(cumulative_units) FILTER (WHERE city = 'London') AS london_max_cumulative_units,
    SORT_ARRAY(COLLECT_SET(city)) AS city_list,
    RANK() OVER (PARTITION BY DATE_TRUNC('MONTH', sale_date) ORDER BY AVG(units) DESC) AS avg_units_rank
FROM joined_store_sales
GROUP BY DATE_TRUNC('MONTH', sale_date), DATE_TRUNC('WEEK', sale_date)
HAVING london_max_cumulative_units > 0
