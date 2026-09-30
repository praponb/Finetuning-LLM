WITH base AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        store_id,
        product_id,
        quantity,
        DATE_TRUNC('YEAR', last_restock) AS last_restock_period,
        DATE_FORMAT(last_restock, 'yyyy-MM-dd') AS last_restock_label
    FROM inventory
    WHERE store_id = 4
),
deduped AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY store_id ORDER BY last_restock_period, id) AS latest_rank
        FROM base
    ) ranked
    WHERE latest_rank = 1
),
with_dims AS (
    -- Attach stores attributes
    SELECT
        src.id,
        src.store_id,
        src.product_id,
        src.quantity,
        src.last_restock_period,
        src.last_restock_label,
        src.latest_rank,
        d.store_name
    FROM deduped src
    LEFT JOIN stores d
        ON src.store_id = d.id
),
enriched AS (
    -- Attach products attributes
    SELECT
        src.*,
        p.launch_date,
        p.brand,
        p.product_name
    FROM with_dims src
    LEFT JOIN products p
        ON src.product_id = p.id
),
filtered AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        store_id,
        last_restock_period,
        last_restock_label,
        latest_rank,
        store_name,
        launch_date,
        brand,
        product_name,
        DATE_TRUNC('YEAR', last_restock_period) AS last_restock_period_period,
        LENGTH(brand) AS brand_length
    FROM enriched
    WHERE store_id IN (1, 4)
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        id,
        store_id,
        last_restock_label,
        launch_date,
        product_name,
        last_restock_period_period,
        AVG(brand_length) OVER w_grp AS store_id_avg_brand_length,
        ROW_NUMBER() OVER (ORDER BY last_restock_period, id) AS row_seq,
        DENSE_RANK() OVER (PARTITION BY store_id ORDER BY brand_length DESC) AS brand_length_dense_rank
    FROM filtered
    WHERE brand_length <> 78
    WINDOW w_grp AS (PARTITION BY store_id)
),
daily AS (
    -- Grouped metrics
    SELECT
        store_id,
        CASE WHEN row_seq >= 7 THEN 'LARGE' ELSE 'SMALL' END AS row_seq_tier,
        (UNIX_TIMESTAMP(MAX(last_restock_period_period)) - UNIX_TIMESTAMP(MIN(last_restock_period_period))) / 60 AS span_minutes,
        MIN(last_restock_period_period) AS first_last_restock_period_period,
        AVG(row_seq) AS avg_row_seq
    FROM metrics
    GROUP BY store_id, CASE WHEN row_seq >= 7 THEN 'LARGE' ELSE 'SMALL' END
)
SELECT
    s.store_id,
    s.row_seq_tier,
    s.span_minutes,
    s.avg_row_seq,
    (SELECT AVG(s2.units) FROM store_sales s2 WHERE s2.store_id = s.store_id) AS avg_store_sale_units
FROM daily s;
