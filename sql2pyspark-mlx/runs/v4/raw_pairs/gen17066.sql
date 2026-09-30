WITH kept_keys AS (
    -- Set difference on keys
    SELECT product_id, id
    FROM inventory
    EXCEPT
    SELECT product_id, id
    FROM inventory
    WHERE product_id = 5
),
reconciled AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM inventory s
    INNER JOIN kept_keys k ON s.product_id = k.product_id AND s.id = k.id
),
joined AS (
    -- Attach products attributes
    SELECT src.*, p.cost
    FROM reconciled src
    LEFT JOIN products p
        ON src.product_id = p.id
),
with_dims AS (
    -- Attach stores attributes
    SELECT
        s.*,
        d.opened_date,
        d.store_name
    FROM joined s
    INNER JOIN stores d
        ON s.store_id = d.id
),
enriched AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        STDDEV_SAMP(quantity) OVER w_running AS product_id_stddev_samp_quantity,
        MAX(cost) OVER (PARTITION BY store_id, product_id) AS store_id_max_cost,
        RANK() OVER (ORDER BY cost DESC) AS cost_rank
    FROM with_dims
    WINDOW w_running AS (PARTITION BY product_id)
)
SELECT
    s.id,
    s.store_id,
    s.product_id,
    s.quantity,
    s.cost,
    s.store_name,
    s.product_id_stddev_samp_quantity,
    s.cost_rank,
    CASE WHEN store_name RLIKE '^(Airport Kiosk|Old Town)$' THEN 'OTHER_ZONE' WHEN store_name RLIKE '^(Siam Flagship|Downtown Express)$' THEN 'EXTENDED' ELSE 'CORE' END AS store_name_zone,
    (SELECT AVG(x.revenue) FROM store_sales x WHERE x.product_id = s.product_id) AS avg_store_sale_revenue
FROM enriched s
