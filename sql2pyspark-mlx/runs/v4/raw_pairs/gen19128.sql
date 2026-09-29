SELECT
    id,
    store_id,
    product_id,
    sale_date,
    units,
    revenue,
    AVG(units) OVER (PARTITION BY product_id ORDER BY units, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS units_avg_2_prior,
    LEAD(revenue, 2) OVER (PARTITION BY product_id, store_id ORDER BY units, id) AS next_revenue,
    STDDEV_SAMP(units) OVER (PARTITION BY store_id, product_id) AS store_id_stddev_samp_units
FROM store_sales;
