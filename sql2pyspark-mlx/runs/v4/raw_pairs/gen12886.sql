WITH StoreSalesByStore AS (
    SELECT store_id, MAX(revenue) - MIN(revenue) revenue_range, ROUND(AVG(units), 2) avg_units
    FROM store_sales
    GROUP BY store_id
)
SELECT
    stores.sqft,
    stores.opened_date,
    StoreSalesByStore.revenue_range,
    StoreSalesByStore.avg_units
FROM stores
LEFT OUTER JOIN StoreSalesByStore ON stores.id = StoreSalesByStore.store_id
