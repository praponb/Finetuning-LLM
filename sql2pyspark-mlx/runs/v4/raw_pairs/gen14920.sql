select is_active, sum(credit_limit) as total_credit_limit, round(stddev(credit_limit), 2) as stddev_credit_limit from customers group by is_active;
