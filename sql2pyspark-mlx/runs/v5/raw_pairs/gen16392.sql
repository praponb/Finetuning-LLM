SELECT
    id,
    end_date,
    CAST(monthly_fee AS INT) AS monthly_fee_int,
    CAST(monthly_fee AS INT) AS monthly_fee_int_2
FROM subscriptions;
