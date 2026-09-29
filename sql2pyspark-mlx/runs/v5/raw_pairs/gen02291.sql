WITH with_windows AS (
    -- Ranking and gap features
    SELECT
        id,
        store_id,
        units,
        LAG(revenue) OVER (PARTITION BY product_id, store_id ORDER BY units, id) AS prev_revenue,
        LEAD(revenue) OVER (PARTITION BY product_id, store_id ORDER BY units, id) AS next_revenue,
        LEAD(units) OVER (PARTITION BY product_id, store_id ORDER BY units, id) AS next_units,
        LAG(units) OVER (PARTITION BY product_id, store_id ORDER BY units, id) AS prev_units,
        ROW_NUMBER() OVER (PARTITION BY product_id, store_id ORDER BY units, id) AS row_seq
    FROM store_sales
),
matched_keys AS (
    -- Set intersection on keys
    SELECT id, store_id
    FROM with_windows
    INTERSECT ALL
    SELECT id, store_id
    FROM store_sales
    WHERE revenue BETWEEN 2300 AND 3300
),
reconciled AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM with_windows s
    INNER JOIN matched_keys k ON s.id = k.id AND s.store_id = k.store_id
),
enriched AS (
    -- Ranking and gap features
    SELECT *, RANK() OVER (PARTITION BY store_id ORDER BY prev_units NULLS FIRST) AS prev_units_rank
    FROM reconciled
),
enriched_store_sales AS (
    -- Attach stores attributes
    SELECT
        s.store_id,
        s.units,
        s.next_revenue,
        p.opened_date,
        p.region
    FROM enriched s
    LEFT JOIN stores p
        ON s.store_id = p.id
),
with_dims AS (
    -- Attach stores attributes
    SELECT
        a.store_id,
        a.units,
        a.region,
        d.sqft
    FROM enriched_store_sales a
    LEFT JOIN stores d
        ON a.store_id = d.id
    WHERE MONTH(d.opened_date) IN (3, 5, 11)
)
SELECT
    s.store_id,
    s.units,
    s.sqft,
    (SELECT MIN(t2.quantity) FROM inventory t2 WHERE t2.store_id = s.store_id) AS min_inventory_quantity
FROM with_dims s;
