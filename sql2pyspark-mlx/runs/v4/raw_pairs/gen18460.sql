WITH valid AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        store_id,
        product_id,
        sale_date,
        units,
        revenue,
        YEAR(sale_date) AS sale_date_year
    FROM store_sales
    WHERE units < 25
),
deduped AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY store_id, product_id ORDER BY sale_date_year DESC NULLS LAST, id) AS rn
        FROM valid
    ) x
    WHERE rn = 1
),
valid_store_sales AS (
    -- Keep recent, valid rows
    SELECT *, ROUND(revenue, 1) AS revenue_rounded
    FROM deduped
    WHERE sale_date >= TO_DATE('2023-10-09')
      AND units = 37
),
deduped_store_sales AS (
    -- QUALIFY-style window filter
    SELECT *, RANK() OVER (PARTITION BY product_id ORDER BY revenue_rounded DESC NULLS LAST) AS dedup_rank
    FROM valid_store_sales
    QUALIFY dedup_rank = 1
),
kept_keys AS (
    -- Set difference on keys
    SELECT product_id, id
    FROM deduped_store_sales
    EXCEPT ALL
    SELECT product_id, id
    FROM store_sales
    WHERE units IS NULL
),
reconciled AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM deduped_store_sales s
    INNER JOIN kept_keys k ON s.product_id = k.product_id AND s.id = k.id
),
scored AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        store_id,
        product_id,
        sale_date,
        units,
        revenue,
        sale_date_year,
        rn,
        revenue_rounded,
        dedup_rank,
        STDDEV_SAMP(sale_date_year) OVER (PARTITION BY product_id) AS product_id_stddev_samp_sale_date_year,
        RANK() OVER (PARTITION BY product_id ORDER BY revenue NULLS LAST) AS revenue_rank,
        RANK() OVER (PARTITION BY product_id ORDER BY units DESC) AS units_rank,
        MIN(sale_date_year) OVER (PARTITION BY store_id) AS store_id_min_sale_date_year
    FROM reconciled
),
metrics AS (
    -- Ranking and gap features
    SELECT
        id,
        store_id,
        product_id,
        sale_date,
        units,
        revenue,
        sale_date_year,
        rn,
        revenue_rounded,
        dedup_rank,
        product_id_stddev_samp_sale_date_year,
        revenue_rank,
        units_rank,
        store_id_min_sale_date_year,
        ROUND(units / SUM(units) OVER (PARTITION BY store_id), 4) AS units_share,
        RANK() OVER (PARTITION BY product_id ORDER BY revenue_rank DESC NULLS FIRST) AS revenue_rank_rank
    FROM scored
)
-- Multi-level aggregation
SELECT
    product_id,
    DATE_TRUNC('WEEK', sale_date) AS week_start,
    DATE_TRUNC('YEAR', sale_date) AS year_start,
    PERCENTILE_APPROX(revenue_rank_rank, ARRAY(0.25, 0.5, 0.75)) AS revenue_rank_rank_quartiles,
    MIN(store_id_min_sale_date_year) AS min_store_id_min_sale_date_year,
    BIT_OR(units) AS units_bit_or
FROM metrics
GROUP BY product_id, DATE_TRUNC('WEEK', sale_date), DATE_TRUNC('YEAR', sale_date)
HAVING BIT_OR(units) < 1
ORDER BY year_start
