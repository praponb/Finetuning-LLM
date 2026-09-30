with RankedCustomers as (
    select
        id,
        city,
        credit_limit,
        is_active,
        row_number() over (partition by city order by credit_limit, id) as rnk
    from customers
)
select
    id,
    city,
    credit_limit,
    is_active
from RankedCustomers
where rnk <= 5
