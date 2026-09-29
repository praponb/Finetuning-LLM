SELECT
    is_active,
    signup_date,
    segment,
    CONCAT_WS(',', country, segment) country_segment_key
FROM customers
WHERE segment <> 'SMB' AND signup_date BETWEEN '2025-03-01' AND '2025-06-01';
