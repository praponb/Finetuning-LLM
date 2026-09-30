SELECT
    id,
    store_id,
    units,
    sale_date
FROM (
    SELECT
        id,
        store_id,
        units,
        sale_date,
        ROW_NUMBER() OVER (PARTITION BY store_id ORDER BY units DESC, id) rnk
    FROM store_sales
    WHERE revenue >= 1700
) ranked
WHERE rnk = 1
