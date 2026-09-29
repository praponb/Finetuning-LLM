SELECT
    paid_at,
    status,
    method,
    CAST(paid_at AS DATE) paid_at_day,
    DATE_TRUNC('YEAR', paid_at) paid_at_period
FROM payments
ORDER BY paid_at_period;
