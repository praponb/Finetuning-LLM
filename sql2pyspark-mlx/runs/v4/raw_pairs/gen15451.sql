SELECT
    id,
    segment,
    name,
    COALESCE(email, 'Unknown') AS email_filled
FROM customers
WHERE segment = 'Corporate' AND country <> 'TH'
ORDER BY segment ASC
