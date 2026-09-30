WITH lines AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        coupon_codes,
        pos,
        item.product_id,
        item.promo
    FROM carts
    LATERAL VIEW POSEXPLODE(items) li AS pos, item
),
latest AS (
    -- QUALIFY-style window filter
    SELECT *, ROW_NUMBER() OVER (PARTITION BY promo ORDER BY pos NULLS LAST, id, pos) AS row_num
    FROM lines
    QUALIFY row_num = 1
),
cleaned AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        coupon_codes,
        pos,
        product_id,
        promo,
        row_num,
        pos + row_num AS pos_plus_row_num,
        COALESCE(promo, 'OTHER') AS promo_clean
    FROM latest
    WHERE product_id >= 4
),
valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        pos - product_id AS pos_minus_product_id,
        COALESCE(promo, 'OTHER') AS promo_filled
    FROM cleaned
    WHERE row_num <> 1
),
remaining_keys AS (
    -- Set difference on keys
    SELECT id
    FROM valid
    EXCEPT
    SELECT id
    FROM carts
    WHERE customer_id IS NULL
),
kept_rows AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM valid s
    JOIN remaining_keys k ON s.id = k.id
),
enriched AS (
    -- Running and moving aggregates
    SELECT
        *,
        SUM(pos) OVER (PARTITION BY promo) AS promo_sum_pos,
        RANK() OVER w_running AS product_id_rank,
        CUME_DIST() OVER (PARTITION BY promo ORDER BY product_id NULLS FIRST) AS product_id_cume_dist
    FROM kept_rows
    WINDOW w_running AS (ORDER BY product_id)
),
scored AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        AVG(pos) OVER (PARTITION BY promo) AS promo_avg_pos,
        RANK() OVER (ORDER BY product_id_rank) AS product_id_rank_rank,
        CUME_DIST() OVER (ORDER BY product_id NULLS LAST) AS product_id_cume_dist_2
    FROM enriched
)
-- Grouped metrics
SELECT
    promo,
    CASE WHEN promo_avg_pos >= 4 THEN 'HIGH' ELSE 'LOW' END AS promo_avg_pos_tier,
    CASE WHEN product_id_cume_dist_2 >= 0.63 THEN 'PREMIUM' ELSE 'STANDARD' END AS product_id_cume_dist_2_tier,
    COUNT(CASE WHEN promo = 'WELCOME5' THEN 1 END) AS welcome5_count,
    SIZE(COLLECT_SET(promo)) AS num_promos,
    GROUPING(promo) AS gid
FROM scored
GROUP BY GROUPING SETS ((promo, CASE WHEN promo_avg_pos >= 4 THEN 'HIGH' ELSE 'LOW' END, CASE WHEN product_id_cume_dist_2 >= 0.63 THEN 'PREMIUM' ELSE 'STANDARD' END), (promo, CASE WHEN promo_avg_pos >= 4 THEN 'HIGH' ELSE 'LOW' END))
