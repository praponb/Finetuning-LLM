SELECT
    src.*,
    d.sqft,
    d.opened_date
FROM orders src
JOIN stores d
    ON src.store_id = d.id;
