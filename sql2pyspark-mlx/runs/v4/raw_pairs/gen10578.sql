select quarter(signup_date) signup_quarter, sum(credit_limit) total_credit_limit, round(sum(credit_limit) * 100.0 / sum(sum(credit_limit)) over (), 2) as pct_of_total
from customers
group by quarter(signup_date)
