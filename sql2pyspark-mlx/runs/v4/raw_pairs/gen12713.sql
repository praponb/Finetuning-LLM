SELECT
    a.*,
    d.sqft,
    d.store_name
FROM store_sales a
LEFT JOIN stores d
    ON a.store_id = d.id
