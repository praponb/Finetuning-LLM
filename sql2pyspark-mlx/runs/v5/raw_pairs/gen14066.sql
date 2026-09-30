select id, country, age, segment, max(age) over (partition by country) as country_max_age, rank() over (partition by segment order by age desc) as rnk from customers order by age desc
