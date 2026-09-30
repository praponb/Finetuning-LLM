SELECT
    id,
    shipped_date,
    delivered_date,
    CASE WHEN carrier IN ('Kerry', 'Flash') THEN 'Tier 1' ELSE 'Other' END AS carrier_group,
    SUBSTRING(status, 1, 2) AS status_prefix
FROM shipments
