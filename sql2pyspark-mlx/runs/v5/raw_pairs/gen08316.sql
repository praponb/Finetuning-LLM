WITH Src AS (
    SELECT *
    FROM orders
    WHERE store_id = 9
),
channel_summary AS (
    SELECT channel, SUM(CASE WHEN status = 'CANCELLED' THEN 1 ELSE 0 END) AS cancelled_count
    FROM Src
    GROUP BY channel
)
SELECT *
FROM channel_summary
WHERE cancelled_count < 69;
