with Ranked as (
    select
        id,
        city,
        signup_date,
        row_number() over (partition by city order by signup_date desc, id) as row_num
    from customers
)
select *
from Ranked
where row_num <= 1
