select signup_date from customers where year(signup_date) >= 2024 union all select signup_date from customers where credit_limit < 12200 order by signup_date
