WITH cleaned AS (
    SELECT
        id,
        store_id,
        product_id,
        sale_date,
        units,
        revenue + units revenue_per_unit
    FROM store_sales
    WHERE units > 34
),
scored AS (
    SELECT
        *,
        PERCENT_RANK() OVER (PARTITION BY store_id ORDER BY units DESC NULLS LAST) units_percent_rank,
        NTILE(5) OVER (PARTITION BY product_id ORDER BY units, id) units_bucket
    FROM cleaned
),
valid AS (
    SELECT
        id,
        product_id,
        units,
        revenue_per_unit,
        units_percent_rank,
        units_bucket,
        ROUND(units_percent_rank / units, 2) units_percent_rank_per_units,
        units / NULLIF(units_percent_rank, 0) units_per_units_percent_rank
    FROM scored
    WHERE units_percent_rank <= 0.63
),
deduped AS (
    SELECT *
    FROM valid
    QUALIFY ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY units_percent_rank_per_units DESC, id) = 1
),
joined AS (
    SELECT
        src.*,
        p.cost,
        p.stock_qty,
        p.brand
    FROM deduped src
    LEFT JOIN products p
        ON src.product_id = p.id
),
latest AS (
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY brand ORDER BY units_per_units_percent_rank DESC NULLS LAST, id) AS row_num
        FROM joined
    ) t
    WHERE row_num = 1
)
SELECT
    s.id,
    s.product_id,
    s.units,
    s.revenue_per_unit,
    s.units_percent_rank,
    s.units_bucket,
    s.units_per_units_percent_rank,
    s.cost,
    s.stock_qty,
    (SELECT AVG(s2.units) FROM store_sales s2 WHERE s2.product_id = s.product_id) AS avg_store_sale_units
FROM latest s
ORDER BY cost DESC;
