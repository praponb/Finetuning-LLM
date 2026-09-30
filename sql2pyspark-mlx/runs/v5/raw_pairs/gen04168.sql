SELECT
    id,
    units,
    store_id,
    revenue
FROM store_sales
WHERE units > (SELECT AVG(units) FROM store_sales);
