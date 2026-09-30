select quarter(c.signup_date) signup_quarter, c.country, count(*) cnt from accounts a left join customers c on c.id = a.customer_id group by quarter(c.signup_date), c.country order by country desc;
