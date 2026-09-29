WITH cleaned AS (
    -- Keep recent, valid rows
    SELECT *, LAST_DAY(opened_date) AS opened_date_month_end
    FROM stores
    WHERE region IS NOT NULL
      AND opened_date >= TO_DATE('2023-03-13')
),
with_latest AS (
    -- Latest matching inventory per row (LATERAL ... LIMIT 1)
    SELECT s.*, latest.quantity
    FROM cleaned s
    LEFT JOIN LATERAL (
            SELECT c.quantity
            FROM inventory c
            WHERE c.store_id = s.id
              AND c.last_restock <= s.opened_date
            ORDER BY c.last_restock DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
),
with_last AS (
    -- Latest matching store_sale per row (LATERAL ... LIMIT 1)
    SELECT s.*, top1.revenue
    FROM with_latest s
    LEFT JOIN LATERAL (
            SELECT c.revenue
            FROM store_sales c
            WHERE c.store_id = s.id
            ORDER BY c.revenue DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
),
with_last_stores AS (
    -- Latest matching store_sale per row (LATERAL ... LIMIT 1)
    SELECT
        s.*,
        latest.store_sale_revenue,
        latest.store_sale_units
    FROM with_last s
    LEFT JOIN LATERAL (
            SELECT c.revenue AS store_sale_revenue, c.units AS store_sale_units
            FROM store_sales c
            WHERE c.store_id = s.id
            ORDER BY c.revenue DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
),
scored AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        store_name,
        city,
        sqft,
        opened_date_month_end,
        quantity,
        store_sale_revenue,
        store_sale_units,
        AVG(store_sale_units) OVER w_ordered AS running_avg_store_sale_units,
        MAX(store_sale_units) OVER w_ordered AS running_max_store_sale_units,
        COUNT(store_sale_units) OVER w_ordered AS running_count_store_sale_units,
        SUM(store_sale_units) OVER w_ordered AS running_sum_store_sale_units,
        ROW_NUMBER() OVER w_part AS row_seq
    FROM with_last_stores
    WINDOW w_ordered AS (PARTITION BY region, city ORDER BY quantity, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), w_part AS (ORDER BY store_sale_revenue, id)
)
-- Aggregate with conditional metrics
SELECT
    DATE_TRUNC('QUARTER', opened_date_month_end) AS quarter_start,
    city,
    ROUND(AVG(running_sum_store_sale_units), 2) AS avg_running_sum_store_sale_units,
    AVG(running_sum_store_sale_units) AS avg_running_sum_store_sale_units_2,
    AVG(sqft) AS avg_sqft,
    SORT_ARRAY(COLLECT_SET(city)) AS city_list,
    COUNT_IF(city = 'New York') AS new_york_events,
    COUNT(CASE WHEN city = 'Chiang Mai' THEN 1 END) AS chiang_mai_count
FROM scored
GROUP BY DATE_TRUNC('QUARTER', opened_date_month_end), city
