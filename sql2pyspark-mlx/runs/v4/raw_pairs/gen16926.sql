SELECT
    *,
    ROW_NUMBER() OVER (ORDER BY paid_at DESC, id) AS event_seq,
    COUNT(amount) OVER (PARTITION BY status) AS status_count_amount,
    STDDEV_SAMP(amount) OVER (PARTITION BY method) AS method_stddev_samp_amount,
    SUM(amount) OVER (PARTITION BY method) AS method_sum_amount
FROM payments
