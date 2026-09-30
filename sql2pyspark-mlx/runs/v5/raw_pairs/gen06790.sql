WITH ranked AS (
    SELECT *, RANK() OVER (PARTITION BY order_id ORDER BY unit_price NULLS LAST) AS rn
    FROM order_items
    QUALIFY rn = 1
),
deduped AS (
    SELECT
        id,
        order_id,
        product_id,
        quantity,
        unit_price,
        rn
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY rn DESC, id) AS rn_2
        FROM ranked
    ) x
    WHERE rn_2 = 1
),
enriched AS (
    SELECT
        src.*,
        p.cost,
        p.brand,
        p.price
    FROM deduped src
    INNER JOIN products p
        ON src.product_id = p.id
    WHERE p.supplier_id IS NULL
),
remaining_keys AS (
    SELECT product_id, id
    FROM enriched
    INTERSECT ALL
    SELECT product_id, id
    FROM order_items
    WHERE unit_price >= 200
),
reconciled AS (
    SELECT s.*
    FROM enriched s
    INNER JOIN remaining_keys k ON s.product_id = k.product_id AND s.id = k.id
),
enriched_order_items AS (
    SELECT s.*, p.product_name
    FROM reconciled s
    LEFT JOIN products p
        ON s.product_id = p.id
),
joined AS (
    SELECT
        e.*,
        p.price AS product_price,
        p.launch_date,
        p.stock_qty
    FROM enriched_order_items e
    INNER JOIN products p
        ON e.product_id = p.id
),
totals AS (
    SELECT
        order_id,
        SUM(CASE WHEN brand = 'Initech' THEN product_price ELSE 0 END) AS initech_product_price,
        COUNT(CASE WHEN brand = 'Stark' THEN 1 END) AS stark_count,
        CORR(quantity, unit_price) AS corr_quantity_unit_price,
        AVG(product_price) FILTER (WHERE brand = 'Acme') AS acme_avg_product_price,
        COUNT(CASE WHEN brand = 'Stark' THEN 1 END) AS stark_count_2,
        ROUND(AVG(unit_price), 2) AS avg_unit_price
    FROM joined
    GROUP BY order_id
)
SELECT
    s.order_id,
    s.corr_quantity_unit_price,
    s.stark_count_2,
    (SELECT MAX(s2.cost) FROM shipments s2 WHERE s2.order_id = s.order_id) AS max_shipment_cost
FROM totals s
ORDER BY stark_count_2 NULLS LAST;
