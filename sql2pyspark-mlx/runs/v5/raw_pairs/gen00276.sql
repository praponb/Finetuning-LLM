with ranked as (
    select *, rank() over (partition by city order by signup_date desc) as rnk
    from customers
    where email like '%@yahoo.com'
)
select id, city, signup_date
from ranked
where rnk <= 2
