SELECT s.*
FROM customers s
WHERE EXISTS (
        SELECT 1
        FROM subscriptions x
        WHERE x.customer_id = s.id
          AND x.start_date >= s.signup_date
    );
