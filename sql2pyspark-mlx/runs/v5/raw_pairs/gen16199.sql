WITH cleaned AS (
    SELECT *, CASE WHEN quantity >= 258 THEN 'High' WHEN quantity >= 90 THEN 'Medium' ELSE 'Low' END AS quantity_band
    FROM inventory
    WHERE last_restock > TO_DATE('2023-07-15')
      AND store_id IN (1, 2, 3, 4)
),
with_dims AS (
    SELECT
        e.*,
        b.sqft,
        b.city,
        b.opened_date
    FROM cleaned e
    LEFT JOIN stores b
        ON e.store_id = b.id
),
enriched AS (
    SELECT
        a.id,
        a.product_id,
        a.quantity,
        a.last_restock,
        a.quantity_band,
        a.opened_date,
        b.stock_qty,
        b.price,
        b.cost
    FROM with_dims a
    LEFT JOIN products b
        ON a.product_id = b.id
),
deduped AS (
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY last_restock DESC, id) AS dedup_rank
        FROM enriched
    ) t
    WHERE dedup_rank = 1
),
filtered AS (
    SELECT
        id,
        product_id,
        quantity,
        last_restock,
        quantity_band,
        opened_date,
        stock_qty,
        price,
        cost,
        CEIL(stock_qty) AS stock_qty_whole,
        WEEKOFYEAR(opened_date) AS opened_date_weekofyear
    FROM deduped
    WHERE last_restock > DATE '2023-03-01'
      AND stock_qty > 358
)
SELECT
    product_id,
    DATE_TRUNC('WEEK', last_restock) AS week_start,
    COUNT(DISTINCT id) AS distinct_ids,
    APPROX_PERCENTILE(quantity, ARRAY(0.25, 0.5, 0.75)) AS quantity_quartiles,
    PERCENT_RANK() OVER (PARTITION BY product_id ORDER BY COUNT(DISTINCT id) DESC) AS distinct_ids_percent_rank
FROM filtered
GROUP BY product_id, week_start
ORDER BY distinct_ids DESC
