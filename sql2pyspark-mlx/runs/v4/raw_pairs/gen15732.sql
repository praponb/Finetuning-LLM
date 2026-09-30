SELECT
    id,
    paid_at,
    method,
    RANK() OVER (ORDER BY paid_at) AS rnk,
    ROW_NUMBER() OVER (PARTITION BY method ORDER BY paid_at, id) AS paid_at_row
FROM payments;
