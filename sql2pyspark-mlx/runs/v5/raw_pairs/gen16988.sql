WITH matched_keys AS (
    -- Set difference on keys
    SELECT supplier_id, brand
    FROM products
    EXCEPT DISTINCT
    SELECT supplier_id, brand
    FROM products
    WHERE stock_qty BETWEEN 151 AND 305
),
verified AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM products s
    INNER JOIN matched_keys k ON s.supplier_id = k.supplier_id AND s.brand = k.brand
),
enriched AS (
    -- Attach suppliers attributes
    SELECT
        s.brand,
        s.cost,
        s.tags,
        s.supplier_id,
        p2.supplier_name,
        p2.country,
        p2.city
    FROM verified s
    INNER JOIN suppliers p2
        ON s.supplier_id = p2.id
    WHERE p2.rating > 2.5
),
enriched_products AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        PERCENT_RANK() OVER (PARTITION BY city, supplier_id ORDER BY cost NULLS LAST) AS cost_percent_rank,
        PERCENT_RANK() OVER (PARTITION BY supplier_id ORDER BY cost DESC NULLS FIRST) AS cost_percent_rank_2
    FROM enriched
),
with_dims AS (
    -- Attach suppliers attributes
    SELECT
        src.*,
        b.country AS supplier_country,
        b.rating,
        b.supplier_name AS supplier_supplier_name
    FROM enriched_products src
    INNER JOIN suppliers b
        ON src.supplier_id = b.id
),
kept_keys AS (
    -- Set intersection on keys
    SELECT brand, supplier_id
    FROM with_dims
    INTERSECT
    SELECT brand, supplier_id
    FROM products
    WHERE category = 'Fashion'
),
verified_products AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM with_dims s
    INNER JOIN kept_keys k ON s.brand = k.brand AND s.supplier_id = k.supplier_id
),
joined AS (
    -- Attach suppliers attributes
    SELECT
        s.brand,
        s.cost,
        s.supplier_id,
        s.supplier_name,
        s.country,
        s.city,
        s.cost_percent_rank,
        s.cost_percent_rank_2,
        s.supplier_country,
        s.rating,
        s.supplier_supplier_name,
        p2.city AS supplier_city
    FROM verified_products s
    INNER JOIN suppliers p2
        ON s.supplier_id = p2.id
),
enriched_products_2 AS (
    -- Attach suppliers attributes
    SELECT src.*, b.rating AS supplier_rating
    FROM joined src
    INNER JOIN suppliers b
        ON src.supplier_id = b.id
)
SELECT
    s.brand,
    s.supplier_id,
    s.supplier_name,
    s.city,
    s.cost_percent_rank,
    s.cost_percent_rank_2,
    s.supplier_country,
    s.supplier_city,
    s.supplier_rating,
    CASE WHEN supplier_name RLIKE '^(Tokyo Traders|Global Supply)$' THEN 'EXTENDED' WHEN supplier_name RLIKE '^(Siam Parts Co|EuroGoods GmbH)$' THEN 'OTHER_ZONE' ELSE 'GROUP_A' END AS supplier_name_zone,
    (SELECT MAX(t2.price) FROM products t2 WHERE t2.supplier_id = s.supplier_id) AS max_product_price
FROM enriched_products_2 s;
