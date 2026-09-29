select month(signup_date) signup_month, round(avg(age), 2) avg_age, avg(credit_limit) avg_credit_limit from customers group by 1
