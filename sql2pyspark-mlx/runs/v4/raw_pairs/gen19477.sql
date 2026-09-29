SELECT
    id,
    store_name,
    DATEDIFF(CURRENT_DATE(), opened_date) AS days_since_opened_date,
    COALESCE(sqft, 0) AS sqft_filled
FROM stores
WHERE sqft <= 5086 AND region = 'East'
ORDER BY store_name DESC, days_since_opened_date DESC
