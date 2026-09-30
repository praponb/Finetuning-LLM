select *, greatest(age, credit_limit) as greatest_age_credit_limit, initcap(trim(country)) as country_title from customers where signup_date < '2023-07-01';
