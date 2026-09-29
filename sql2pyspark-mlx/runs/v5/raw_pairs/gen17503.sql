select country, sum(credit_limit) as total_credit_limit, count(age) as age_count from customers group by country order by country;
