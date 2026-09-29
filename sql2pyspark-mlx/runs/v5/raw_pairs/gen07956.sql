WITH cleaned AS (
    SELECT *, ROUND(units / revenue, 2) AS revenue_per_unit
    FROM store_sales
    WHERE sale_date > TO_DATE('2023-03-08')
),
scored AS (
    SELECT
        *,
        COUNT(units) OVER (PARTITION BY product_id ORDER BY revenue_per_unit ASC, id ASC ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS last4_count_units,
        SUM(units) OVER (PARTITION BY product_id ORDER BY revenue_per_unit ASC, id ASC ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS last4_sum_units,
        AVG(units) OVER (PARTITION BY product_id ORDER BY revenue_per_unit ASC, id ASC ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS last4_avg_units
    FROM cleaned
),
ranked AS (
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY store_id ORDER BY last4_count_units DESC NULLS LAST, id ASC) AS dedup_rank
        FROM scored
    ) t
    WHERE dedup_rank = 1
),
metrics AS (
    SELECT
        id,
        product_id,
        units,
        revenue,
        revenue_per_unit,
        last4_count_units,
        dedup_rank,
        NTILE(5) OVER w_grp AS last4_count_units_bucket,
        AVG(last4_avg_units) OVER (PARTITION BY product_id) AS product_id_avg_last4_avg_units,
        LEAD(last4_count_units, 2) OVER (PARTITION BY store_id ORDER BY dedup_rank ASC, id ASC) AS next_last4_count_units
    FROM ranked
    WINDOW w_grp AS (PARTITION BY store_id ORDER BY last4_count_units ASC, id ASC)
),
joined AS (
    SELECT
        src.*,
        b.product_name,
        b.price,
        b.category
    FROM metrics src
    JOIN products b
        ON src.product_id = b.id
),
deduped AS (
    SELECT
        id,
        product_id,
        dedup_rank,
        last4_count_units_bucket,
        next_last4_count_units,
        price,
        category
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY product_id, category ORDER BY last4_count_units DESC, id ASC) AS row_num
        FROM joined
    ) x
    WHERE row_num = 1
),
valid AS (
    SELECT
        id,
        product_id,
        dedup_rank,
        last4_count_units_bucket,
        price,
        category,
        LEAST(dedup_rank, last4_count_units_bucket) AS least_dedup_rank_last4_count_units_bucket,
        COALESCE(category, 'OTHER') AS category_filled
    FROM deduped
    WHERE category IN ('Books', 'Electronics')
)
SELECT
    s.id,
    s.product_id,
    s.dedup_rank,
    s.last4_count_units_bucket,
    s.price,
    s.category,
    s.least_dedup_rank_last4_count_units_bucket,
    s.category_filled,
    (SELECT MIN(x.quantity) FROM inventory x WHERE x.product_id = s.product_id) AS min_inventory_quantity
FROM valid s
ORDER BY min_inventory_quantity DESC
