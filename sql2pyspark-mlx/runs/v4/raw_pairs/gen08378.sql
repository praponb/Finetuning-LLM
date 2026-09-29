with ranked_customers as (select *, row_number() over (partition by country order by age, id) rnk from customers) select id, country, age, city from ranked_customers where rnk <= 2 order by city
