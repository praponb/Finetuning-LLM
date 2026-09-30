SELECT
    accounts.status,
    customers.city,
    customers.credit_limit,
    customers.signup_date,
    COALESCE(customers.name, 'unassigned') AS name_filled
FROM accounts
INNER JOIN customers ON customers.id = accounts.customer_id;
