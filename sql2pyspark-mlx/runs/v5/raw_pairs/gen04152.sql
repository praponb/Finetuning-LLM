select *, upper(email) as email_upper, initcap(trim(email)) as email_title from customers where signup_date <= '2023-08-01' and country <> 'US' and year(signup_date) >= 2025 order by name desc
