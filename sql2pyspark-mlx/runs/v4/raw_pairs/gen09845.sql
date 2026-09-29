SELECT o.id, o.channel, c.city, c.email, LAST_DAY(c.signup_date) signup_date_month_end FROM orders o INNER JOIN customers c ON o.customer_id = c.id
