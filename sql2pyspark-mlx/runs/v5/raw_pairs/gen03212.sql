with ranked_accounts as (
    select
        id,
        status,
        opened_date,
        customer_id,
        row_number() over (partition by status order by opened_date desc, id) as rn
    from accounts
)
select
    id,
    status,
    opened_date,
    customer_id
from ranked_accounts
where rn <= 2;
