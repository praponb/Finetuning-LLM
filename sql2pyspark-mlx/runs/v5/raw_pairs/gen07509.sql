WITH with_windows AS (
    SELECT
        id,
        store_id,
        product_id,
        MAX(quantity) OVER w_recent last3_max_quantity,
        SUM(quantity) OVER w_recent last3_sum_quantity,
        MIN(quantity) OVER w_recent last3_min_quantity
    FROM inventory
    WINDOW w_recent AS (PARTITION BY store_id ORDER BY quantity, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW)
),
metrics AS (
    SELECT
        *,
        FIRST_VALUE(last3_max_quantity) IGNORE NULLS OVER (PARTITION BY store_id ORDER BY last3_sum_quantity, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) first_known_last3_max_quantity,
        AVG(last3_sum_quantity) OVER (PARTITION BY store_id ORDER BY last3_min_quantity, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) last3_sum_quantity_avg_6_prior
    FROM with_windows
),
ranked AS (
    SELECT *
    FROM metrics
    QUALIFY ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY first_known_last3_max_quantity NULLS LAST, id) = 1
),
base AS (
    SELECT
        id,
        store_id,
        last3_max_quantity,
        last3_sum_quantity,
        last3_min_quantity,
        first_known_last3_max_quantity,
        first_known_last3_max_quantity / NULLIF(last3_max_quantity, 0) first_known_last3_max_quantity_per_last3_max_quantity,
        last3_sum_quantity + last3_sum_quantity_avg_6_prior last3_sum_quantity_plus_last3_sum_quantity_avg_6_prior
    FROM ranked
    WHERE store_id IN (2, 3, 4, 5)
),
joined AS (
    SELECT
        a.id,
        a.store_id,
        a.last3_sum_quantity,
        a.first_known_last3_max_quantity,
        a.first_known_last3_max_quantity_per_last3_max_quantity,
        a.last3_sum_quantity_plus_last3_sum_quantity_avg_6_prior,
        b.city,
        b.opened_date
    FROM base a
    INNER JOIN stores b
        ON a.store_id = b.id
    WHERE b.sqft = 5958
),
scored AS (
    SELECT
        *,
        PERCENT_RANK() OVER (PARTITION BY store_id ORDER BY last3_sum_quantity DESC) last3_sum_quantity_percent_rank,
        CUME_DIST() OVER (PARTITION BY city ORDER BY first_known_last3_max_quantity_per_last3_max_quantity DESC) first_known_last3_max_quantity_per_last3_max_quantity_cume_dist
    FROM joined
),
enriched AS (
    SELECT
        src.id,
        src.store_id,
        src.first_known_last3_max_quantity,
        src.first_known_last3_max_quantity_per_last3_max_quantity,
        src.last3_sum_quantity_plus_last3_sum_quantity_avg_6_prior,
        src.city,
        src.opened_date,
        src.first_known_last3_max_quantity_per_last3_max_quantity_cume_dist,
        p.city AS store_city,
        p.opened_date AS store_opened_date
    FROM scored src
    LEFT JOIN stores p
        ON src.store_id = p.id
)
SELECT
    id,
    first_known_last3_max_quantity_per_last3_max_quantity,
    last3_sum_quantity_plus_last3_sum_quantity_avg_6_prior,
    city,
    opened_date,
    first_known_last3_max_quantity_per_last3_max_quantity_cume_dist,
    store_city
FROM enriched
ORDER BY first_known_last3_max_quantity_per_last3_max_quantity_cume_dist
