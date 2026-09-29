WITH with_dims AS (
    -- Attach products attributes
    SELECT s.*, d.brand
    FROM inventory s
    JOIN products d
        ON s.product_id = d.id
),
enriched AS (
    -- Ranking and gap features
    SELECT
        *,
        FIRST_VALUE(brand) IGNORE NULLS OVER (PARTITION BY product_id, brand ORDER BY quantity ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_brand,
        AVG(quantity) OVER (PARTITION BY brand, product_id ORDER BY last_restock ASC, id ASC ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS quantity_avg_3_prior,
        ROW_NUMBER() OVER w_hist AS event_seq,
        ROW_NUMBER() OVER (PARTITION BY store_id ORDER BY last_restock ASC, id ASC) AS seq_no
    FROM with_dims
    WINDOW w_hist AS (PARTITION BY store_id, brand ORDER BY last_restock ASC, id ASC)
),
ranked AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY brand, product_id ORDER BY seq_no DESC NULLS LAST, id ASC) AS rn
        FROM enriched
    ) x
    WHERE rn = 1
),
valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        store_id,
        product_id,
        first_known_brand,
        event_seq,
        seq_no,
        rn,
        COALESCE(quantity_avg_3_prior, 0) AS quantity_avg_3_prior_filled
    FROM ranked
    WHERE quantity >= 198
)
SELECT
    s.id,
    s.store_id,
    s.product_id,
    s.first_known_brand,
    s.event_seq,
    s.rn,
    (SELECT AVG(x.revenue) FROM store_sales x WHERE x.product_id = s.product_id) AS avg_store_sale_revenue
FROM valid s;
