WITH metrics AS (
    -- Running and moving aggregates
    SELECT
        *,
        AVG(revenue) OVER (PARTITION BY product_id ORDER BY units ASC, id ASC ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS revenue_avg_6,
        CORR(units, revenue) OVER (PARTITION BY product_id) AS corr_units_revenue
    FROM store_sales
),
enriched AS (
    -- Attach products attributes
    SELECT
        a.*,
        d.brand,
        d.price
    FROM metrics a
    LEFT JOIN products d
        ON a.product_id = d.id
),
remaining_keys AS (
    -- Set difference on keys
    SELECT product_id, store_id
    FROM enriched
    EXCEPT
    SELECT product_id, store_id
    FROM store_sales
    WHERE product_id IN (3, 6, 7, 10)
),
verified AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM enriched s
    INNER JOIN remaining_keys k ON s.product_id = k.product_id AND s.store_id = k.store_id
),
base AS (
    -- Keep recent, valid rows
    SELECT
        *,
        DATE_SUB(sale_date, 32) AS sale_date_minus_32d,
        SUBSTRING(brand, 1, 4) AS brand_prefix
    FROM verified
    WHERE brand IS NOT NULL
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        *,
        CORR(revenue, units) OVER (PARTITION BY product_id) AS corr_revenue_units,
        PERCENT_RANK() OVER (ORDER BY units DESC NULLS LAST) AS units_percent_rank,
        RANK() OVER (ORDER BY units DESC NULLS FIRST) AS units_rank,
        RANK() OVER (PARTITION BY brand, store_id ORDER BY corr_units_revenue DESC NULLS LAST) AS corr_units_revenue_rank,
        RANK() OVER (ORDER BY revenue_avg_6 ASC NULLS LAST) AS revenue_avg_6_rank
    FROM base
)
SELECT
    s.id,
    s.product_id,
    s.units,
    s.revenue,
    s.revenue_avg_6,
    s.corr_units_revenue,
    s.brand,
    s.price,
    s.sale_date_minus_32d,
    (SELECT AVG(t2.revenue) FROM store_sales t2 WHERE t2.product_id = s.product_id) AS avg_store_sale_revenue
FROM with_windows s
