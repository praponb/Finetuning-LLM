WITH filtered AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        supplier_name,
        country,
        city,
        rating,
        CONCAT(city, '/', supplier_name) AS city_supplier_name,
        REGEXP_REPLACE(supplier_name, ' ', '_') AS supplier_name_slug,
        COALESCE(city, 'OTHER') AS city_clean
    FROM suppliers
    WHERE city IS DISTINCT FROM 'London'
),
with_latest AS (
    -- Latest matching product per row (LATERAL ... LIMIT 1)
    SELECT
        s.*,
        top1.brand,
        top1.price
    FROM filtered s
    LEFT JOIN LATERAL (
            SELECT c.brand, c.price
            FROM products c
            WHERE c.supplier_id = s.id
            ORDER BY c.cost DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
),
base AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        supplier_name,
        country,
        city,
        rating,
        city_supplier_name,
        supplier_name_slug,
        price,
        price * rating AS price_times_rating
    FROM with_latest
    WHERE rating <= 2.8
),
enriched AS (
    -- Running and moving aggregates
    SELECT
        id,
        rating,
        city_supplier_name,
        supplier_name_slug,
        price,
        LAG(price_times_rating) OVER w_grp AS prev_price_times_rating,
        LEAD(price_times_rating) OVER w_grp AS next_price_times_rating,
        LEAD(price) OVER w_grp AS next_price,
        ROW_NUMBER() OVER w_grp AS seq_no,
        NTILE(4) OVER (PARTITION BY country, city ORDER BY rating, id) AS rating_bucket
    FROM base
    WINDOW w_grp AS (PARTITION BY city ORDER BY rating, id)
),
enriched_suppliers AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        supplier_name_slug,
        prev_price_times_rating,
        next_price_times_rating,
        seq_no,
        MIN(rating) OVER w_ordered AS running_min_rating,
        MAX(rating) OVER w_ordered AS running_max_rating,
        COUNT(rating) OVER w_ordered AS running_count_rating,
        AVG(rating) OVER w_ordered AS running_avg_rating
    FROM enriched
    WINDOW w_ordered AS (ORDER BY seq_no, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
kept AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM enriched_suppliers s
    WHERE EXISTS (
            SELECT 1
            FROM products x
            WHERE x.supplier_id = s.id
              AND x.stock_qty = 123
        )
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        id,
        supplier_name_slug,
        prev_price_times_rating,
        next_price_times_rating,
        running_max_rating,
        running_avg_rating,
        LEAD(running_min_rating, 2) OVER (ORDER BY running_avg_rating, id) AS next_running_min_rating,
        RANK() OVER (ORDER BY running_count_rating DESC NULLS FIRST) AS running_count_rating_rank,
        PERCENT_RANK() OVER (ORDER BY next_price_times_rating NULLS LAST) AS next_price_times_rating_percent_rank,
        SUM(next_price_times_rating) OVER (ORDER BY seq_no, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_next_price_times_rating
    FROM kept
)
-- Native SQL UNPIVOT (wide to long)
SELECT *
FROM (
    SELECT
        id,
        running_max_rating,
        next_price_times_rating,
        running_avg_rating
    FROM with_windows
) src
UNPIVOT INCLUDE NULLS (
    reading FOR measure IN (running_max_rating AS RUNNING_MAX_RATING, next_price_times_rating AS NEXT_PRICE_TIMES_RATING, running_avg_rating AS RUNNING_AVG_RATING_VAL)
);
