SELECT
    id,
    status,
    end_date,
    ROW_NUMBER() OVER (PARTITION BY status ORDER BY end_date, id) AS row_num
FROM subscriptions;
