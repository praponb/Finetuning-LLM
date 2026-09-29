select s.end_date, c.credit_limit from subscriptions s left join customers c on s.customer_id = c.id where c.country = 'UK' order by credit_limit
