SELECT
    store_name,
    region,
    city,
    ADD_MONTHS(opened_date, 1) opened_date_plus_months
FROM stores
WHERE region IN ('East', 'South', 'West') AND (region IN ('Central', 'South') OR city IN ('Chiang Mai', 'Bangkok', 'London'))
ORDER BY opened_date_plus_months DESC;
