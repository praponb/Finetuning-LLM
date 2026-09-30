WITH valid AS (
    -- Keep recent, valid rows
    SELECT *, ROUND(units * 0.85, 2) units_scaled
    FROM store_sales
    WHERE product_id IN (5, 7, 10)
),
cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        store_id,
        product_id,
        units,
        units_scaled,
        CAST(units AS STRING) units_str
    FROM valid
    WHERE revenue <= 1800
),
scored AS (
    -- Running and moving aggregates
    SELECT
        *,
        SUM(units_scaled) OVER (ORDER BY units_scaled, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) running_units_scaled,
        DENSE_RANK() OVER (PARTITION BY product_id, store_id ORDER BY units_scaled DESC NULLS FIRST) units_scaled_dense_rank,
        SUM(units_scaled) OVER (PARTITION BY product_id ORDER BY units, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) running_units_scaled_2,
        AVG(units) OVER (PARTITION BY store_id, product_id ORDER BY units_scaled, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) units_avg_2_prior,
        LAST_VALUE(units) IGNORE NULLS OVER (PARTITION BY product_id ORDER BY units_scaled, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) last_known_units
    FROM cleaned
),
scored_store_sales AS (
    -- Ranking and gap features
    SELECT
        id,
        store_id,
        product_id,
        units,
        units_scaled,
        units_str,
        running_units_scaled,
        units_scaled_dense_rank,
        running_units_scaled_2,
        units_avg_2_prior,
        last_known_units,
        ROW_NUMBER() OVER (PARTITION BY store_id ORDER BY running_units_scaled_2 DESC, id) event_seq,
        NTILE(5) OVER (PARTITION BY store_id, product_id ORDER BY units_scaled_dense_rank, id) units_scaled_dense_rank_bucket,
        PERCENT_RANK() OVER (PARTITION BY store_id ORDER BY units_scaled DESC NULLS LAST) units_scaled_percent_rank,
        NTILE(10) OVER (PARTITION BY store_id, product_id ORDER BY units_scaled_dense_rank, id) units_scaled_dense_rank_bucket_2,
        ROUND(last_known_units / SUM(last_known_units) OVER (PARTITION BY product_id), 4) last_known_units_share
    FROM scored
),
enriched AS (
    -- Attach products attributes
    SELECT src.*, d.brand
    FROM scored_store_sales src
    INNER JOIN products d
        ON src.product_id = d.id
)
SELECT
    s.id,
    s.store_id,
    s.units,
    s.units_str,
    s.running_units_scaled,
    s.units_scaled_dense_rank,
    s.running_units_scaled_2,
    s.units_avg_2_prior,
    s.event_seq,
    (SELECT AVG(t2.discount) FROM orders t2 WHERE t2.store_id = s.store_id) AS avg_order_discount
FROM enriched s;
