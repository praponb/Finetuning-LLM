WITH cleaned AS (
    SELECT
        id,
        store_id,
        quantity,
        last_restock,
        DATE_TRUNC('WEEK', last_restock) AS last_restock_period
    FROM inventory
    WHERE store_id IN (1, 2, 3, 4)
),
remaining_keys AS (
    SELECT id, store_id
    FROM cleaned
    INTERSECT ALL
    SELECT id, store_id
    FROM inventory
    WHERE store_id IN (2, 3, 4, 6)
),
verified AS (
    SELECT s.*
    FROM cleaned s
    JOIN remaining_keys k ON s.id = k.id AND s.store_id = k.store_id
),
metrics AS (
    SELECT *, SUM(quantity) OVER (PARTITION BY store_id) AS store_id_sum_quantity
    FROM verified
    WHERE last_restock < '2025-01-04'
),
joined AS (
    SELECT
        a.*,
        s.opened_date,
        s.store_name,
        s.region
    FROM metrics a
    LEFT JOIN stores s
        ON a.store_id = s.id
),
joined_inventory AS (
    SELECT
        a.*,
        d.store_name AS store_store_name,
        d.city,
        d.region AS store_region
    FROM joined a
    JOIN stores d
        ON a.store_id = d.id
),
metrics_inventory AS (
    SELECT
        *,
        MAX(store_id_sum_quantity) OVER (PARTITION BY region) AS region_max_store_id_sum_quantity,
        DENSE_RANK() OVER (PARTITION BY store_id ORDER BY store_id_sum_quantity DESC NULLS FIRST) AS store_id_sum_quantity_dense_rank
    FROM joined_inventory
),
metrics_inventory_2 AS (
    SELECT *, RANK() OVER (ORDER BY quantity DESC) AS quantity_rank
    FROM metrics_inventory
)
SELECT
    last_restock,
    store_id_sum_quantity,
    opened_date,
    store_store_name,
    city,
    region_max_store_id_sum_quantity,
    ROUND(quantity_rank / SUM(quantity_rank) OVER (PARTITION BY store_region, store_id), 4) AS quantity_rank_share,
    MAX(quantity_rank) OVER (PARTITION BY city, store_id) AS city_max_quantity_rank,
    AVG(quantity_rank) OVER w_hist AS store_id_avg_quantity_rank
FROM metrics_inventory_2
WINDOW w_hist AS (PARTITION BY store_id);
