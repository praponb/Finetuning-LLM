SELECT
    segment,
    BIT_AND(age) AS age_bit_and,
    STDDEV_SAMP(credit_limit) AS credit_limit_stddev,
    COUNT(DISTINCT id) AS distinct_ids,
    MIN(age) AS min_age,
    SUM(CASE WHEN segment = 'SMB' THEN age ELSE 0 END) AS smb_age,
    GROUPING_ID() AS grouping_id
FROM customers
GROUP BY CUBE (segment)
ORDER BY credit_limit_stddev NULLS FIRST;
