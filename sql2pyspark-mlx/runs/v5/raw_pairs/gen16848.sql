SELECT
    id,
    delivered_date,
    carrier,
    INITCAP(TRIM(status)) AS status_title
FROM shipments;
