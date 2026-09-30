WITH with_dims AS (
    -- Attach products attributes
    SELECT e.*, b.price
    FROM store_sales e
    INNER JOIN products b
        ON e.product_id = b.id
),
base AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        store_id,
        product_id,
        sale_date,
        units,
        price,
        DATE_SUB(sale_date, 36) AS sale_date_minus_36d
    FROM with_dims
    WHERE sale_date >= DATE '2023-05-23'
      AND YEAR(sale_date) = 2023
),
with_dims_store_sales AS (
    -- Attach stores attributes
    SELECT s.*, p.city
    FROM base s
    LEFT JOIN stores p
        ON s.store_id = p.id
),
enriched AS (
    -- Ranking and gap features
    SELECT
        id,
        store_id,
        product_id,
        sale_date,
        units,
        price,
        sale_date_minus_36d,
        city,
        LAST_VALUE(price, TRUE) OVER (PARTITION BY store_id ORDER BY price, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_price,
        ROW_NUMBER() OVER (PARTITION BY city ORDER BY units, id) AS event_seq,
        LEAD(units, 1) OVER w_ordered AS next_units,
        NTILE(3) OVER (PARTITION BY city ORDER BY units DESC, id) AS units_bucket,
        LAST_VALUE(units) IGNORE NULLS OVER (PARTITION BY city ORDER BY sale_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_units
    FROM with_dims_store_sales
    WINDOW w_ordered AS (PARTITION BY city ORDER BY price, id)
),
enriched_store_sales AS (
    -- Running and moving aggregates
    SELECT
        id,
        store_id,
        product_id,
        sale_date,
        units,
        price,
        sale_date_minus_36d,
        city,
        last_known_price,
        event_seq,
        next_units,
        units_bucket,
        last_known_units,
        FIRST_VALUE(price) IGNORE NULLS OVER (PARTITION BY city ORDER BY units, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_price,
        ROUND(last_known_units / SUM(last_known_units) OVER (PARTITION BY store_id), 4) AS last_known_units_share,
        LEAD(next_units, 1) OVER (PARTITION BY store_id ORDER BY sale_date, id) AS next_next_units
    FROM enriched
)
-- Grouped metrics
SELECT
    DATE_TRUNC('MONTH', sale_date_minus_36d) AS activity_month,
    COUNT(CASE WHEN city = 'Berlin' THEN 1 END) AS berlin_count,
    COUNT(*) AS num_rows,
    COUNT_IF(city = 'Berlin') AS berlin_events,
    ROUND(SUM(last_known_price) / NULLIF(COUNT(CASE WHEN city = 'New York' THEN 1 END), 0), 2) AS last_known_price_per_new_york,
    SUM(last_known_units_share) FILTER (WHERE city = 'Bangkok') AS bangkok_total_last_known_units_share
FROM enriched_store_sales
GROUP BY activity_month
ORDER BY bangkok_total_last_known_units_share NULLS LAST;
