WITH enriched AS (
    SELECT
        *,
        SUM(quantity) OVER (PARTITION BY product_id ORDER BY last_restock, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_quantity,
        AVG(quantity) OVER w_running AS quantity_avg_2,
        ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY quantity, id) AS seq_no,
        SUM(quantity) OVER (PARTITION BY product_id ORDER BY quantity, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_quantity_2
    FROM inventory
    WINDOW w_running AS (PARTITION BY store_id ORDER BY last_restock, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW)
),
ranked AS (
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY store_id ORDER BY seq_no, id) AS dedup_rank
        FROM enriched
    ) t
    WHERE dedup_rank = 1
),
enriched_inventory AS (
    SELECT
        src.*,
        p.product_name,
        p.category
    FROM ranked src
    INNER JOIN products p
        ON src.product_id = p.id
),
enriched_inventory_2 AS (
    SELECT
        a.*,
        b.city,
        b.region,
        b.sqft
    FROM enriched_inventory a
    INNER JOIN stores b
        ON a.store_id = b.id
),
enriched_inventory_3 AS (
    SELECT
        *,
        SUM(quantity_avg_2) OVER (PARTITION BY category) AS category_sum_quantity_avg_2,
        SUM(seq_no) OVER (PARTITION BY store_id ORDER BY cumulative_quantity_2, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_seq_no,
        AVG(cumulative_quantity) OVER (PARTITION BY product_id, city) AS product_id_avg_cumulative_quantity,
        RANK() OVER (PARTITION BY product_id ORDER BY quantity NULLS LAST) AS quantity_rank
    FROM enriched_inventory_2
),
with_dims AS (
    SELECT
        e.*,
        d.region AS store_region,
        d.city AS store_city
    FROM enriched_inventory_3 e
    LEFT JOIN stores d
        ON e.store_id = d.id
),
with_dims_inventory AS (
    SELECT
        e.*,
        s.store_name,
        s.city AS store_city_2
    FROM with_dims e
    LEFT JOIN stores s
        ON e.store_id = s.id
)
SELECT
    CASE WHEN product_id_avg_cumulative_quantity >= 1159 THEN 'PREMIUM' ELSE 'STANDARD' END AS product_id_avg_cumulative_quantity_tier,
    SUM(CASE WHEN category = 'Sports' THEN quantity_rank ELSE 0 END) AS sports_quantity_rank,
    APPROX_PERCENTILE(category_sum_quantity_avg_2, ARRAY(0.25, 0.5, 0.75)) AS category_sum_quantity_avg_2_quartiles,
    BIT_AND(quantity) AS quantity_bit_and,
    ROUND(AVG(category_sum_quantity_avg_2), 2) AS avg_category_sum_quantity_avg_2,
    COUNT_IF(store_city = 'Singapore') AS singapore_events,
    SUM(cumulative_quantity) AS total_cumulative_quantity
FROM with_dims_inventory
GROUP BY product_id_avg_cumulative_quantity_tier
