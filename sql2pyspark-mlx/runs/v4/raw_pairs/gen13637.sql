WITH cleaned AS (
    SELECT
        id,
        store_id,
        product_id,
        sale_date,
        units,
        revenue,
        DATE_TRUNC('YEAR', sale_date) AS sale_date_period,
        LAST_DAY(sale_date) AS sale_date_month_end
    FROM store_sales
    WHERE sale_date > TO_DATE('2023-12-08')
      AND sale_date BETWEEN '2025-06-01' AND '2025-10-18'
),
kept_keys AS (
    SELECT product_id, store_id
    FROM cleaned
    INTERSECT
    SELECT product_id, store_id
    FROM store_sales
    WHERE revenue >= 3800
),
kept_rows AS (
    SELECT s.*
    FROM cleaned s
    INNER JOIN kept_keys k ON s.product_id = k.product_id AND s.store_id = k.store_id
),
scored AS (
    SELECT
        *,
        COUNT(units) OVER (PARTITION BY product_id) AS product_id_count_units,
        COUNT(units) OVER w_part AS units_count_30d,
        ROUND(units / SUM(units) OVER (PARTITION BY product_id, store_id), 4) AS units_share,
        PERCENT_RANK() OVER (PARTITION BY product_id ORDER BY revenue DESC) AS revenue_percent_rank
    FROM kept_rows
    WINDOW w_part AS (PARTITION BY store_id ORDER BY CAST(sale_date_period AS BIGINT) RANGE BETWEEN 2592000 PRECEDING AND CURRENT ROW)
),
enriched AS (
    SELECT
        src.*,
        d.store_name,
        d.city
    FROM scored src
    INNER JOIN stores d
        ON src.store_id = d.id
),
matched_keys AS (
    SELECT store_id
    FROM enriched
    EXCEPT ALL
    SELECT store_id
    FROM store_sales
    WHERE revenue < 1500
),
kept_rows_store_sales AS (
    SELECT s.*
    FROM enriched s
    INNER JOIN matched_keys k ON s.store_id = k.store_id
),
scored_store_sales AS (
    SELECT
        *,
        AVG(units_share) OVER (ORDER BY CAST(sale_date_period AS BIGINT) RANGE BETWEEN 604800 PRECEDING AND CURRENT ROW) AS units_share_avg_7d,
        COUNT(units_share) OVER (PARTITION BY store_id) AS store_id_count_units_share,
        SUM(units_count_30d) OVER (PARTITION BY city) AS city_sum_units_count_30d
    FROM kept_rows_store_sales
)
SELECT *
FROM (
    SELECT
        product_id,
        city,
        units
    FROM scored_store_sales
) src
PIVOT (
    SUM(units)
    FOR city IN ('London', 'Tokyo')
);
