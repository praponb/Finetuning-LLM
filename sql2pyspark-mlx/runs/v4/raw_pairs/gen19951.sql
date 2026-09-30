SELECT
    id,
    store_id,
    units,
    DENSE_RANK() OVER (PARTITION BY store_id ORDER BY units DESC) AS dense_rnk
FROM store_sales
ORDER BY store_id, units DESC;
