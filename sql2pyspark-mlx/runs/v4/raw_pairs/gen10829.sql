WITH filtered AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        WEEKOFYEAR(sale_date) AS sale_date_weekofyear,
        revenue + units AS revenue_per_unit
    FROM store_sales
    WHERE sale_date >= DATE '2023-08-02'
)
-- Ranking and gap features
SELECT
    id,
    store_id,
    product_id,
    sale_date,
    units,
    revenue,
    sale_date_weekofyear,
    revenue_per_unit,
    STDDEV_SAMP(sale_date_weekofyear) OVER (PARTITION BY store_id) AS store_id_stddev_samp_sale_date_weekofyear,
    NTILE(3) OVER (PARTITION BY product_id ORDER BY units ASC, id ASC) AS units_bucket,
    AVG(units) OVER (PARTITION BY store_id, product_id ORDER BY sale_date ASC, id ASC ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS units_avg_6_prior
FROM filtered;
