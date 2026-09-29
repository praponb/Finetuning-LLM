with ranked as (
    select
        id,
        customer_id,
        opened_date,
        account_type,
        row_number() over (partition by customer_id order by opened_date desc, id) as rnk
    from accounts
)
select *
from ranked
where rnk = 1
