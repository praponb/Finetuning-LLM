WITH scored AS (
    -- Running and moving aggregates
    SELECT
        *,
        PERCENT_RANK() OVER (ORDER BY unit_price DESC) unit_price_percent_rank,
        SUM(quantity) OVER (PARTITION BY product_id ORDER BY unit_price, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) running_quantity,
        CUME_DIST() OVER (PARTITION BY product_id ORDER BY quantity DESC NULLS FIRST) quantity_cume_dist
    FROM order_items
),
latest AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM scored
    QUALIFY ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY unit_price_percent_rank DESC, id) = 1
),
cleaned AS (
    -- Keep recent, valid rows
    SELECT
        id,
        product_id,
        quantity,
        unit_price_percent_rank,
        quantity_cume_dist,
        ROUND(unit_price / running_quantity, 2) unit_price_per_running_quantity
    FROM latest
    WHERE product_id IS NOT NULL
),
filtered AS (
    -- Keep recent, valid rows
    SELECT
        *,
        LEAST(quantity_cume_dist, quantity) least_quantity_cume_dist_quantity,
        LEAST(quantity, unit_price_per_running_quantity) least_quantity_unit_price_per_running_quantity
    FROM cleaned
    WHERE product_id IS NOT NULL
),
enriched AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        AVG(least_quantity_unit_price_per_running_quantity) OVER (PARTITION BY product_id ORDER BY least_quantity_unit_price_per_running_quantity, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) least_quantity_unit_price_per_running_quantity_avg_2_prior,
        STDDEV_SAMP(least_quantity_unit_price_per_running_quantity) OVER (PARTITION BY product_id) product_id_stddev_samp_least_quantity_unit_price_per_running_quantity,
        MAX(unit_price_percent_rank) OVER (PARTITION BY product_id) product_id_max_unit_price_percent_rank,
        FIRST_VALUE(quantity, TRUE) OVER (PARTITION BY product_id ORDER BY least_quantity_cume_dist_quantity, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) first_known_quantity,
        NTILE(4) OVER (PARTITION BY product_id ORDER BY quantity_cume_dist, id) quantity_cume_dist_bucket
    FROM filtered
)
SELECT
    s.id,
    s.product_id,
    s.quantity,
    s.unit_price_percent_rank,
    s.unit_price_per_running_quantity,
    s.least_quantity_cume_dist_quantity,
    s.least_quantity_unit_price_per_running_quantity,
    s.product_id_max_unit_price_percent_rank,
    s.first_known_quantity,
    (SELECT SUM(s2.units) FROM store_sales s2 WHERE s2.product_id = s.product_id) AS sum_store_sale_units
FROM enriched s
ORDER BY unit_price_per_running_quantity DESC;
