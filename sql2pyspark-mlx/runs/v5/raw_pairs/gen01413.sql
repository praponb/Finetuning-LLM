with dim_totals as (
    select
        dim.status,
        dim.account_type,
        e.merchant,
        count(*) as num_rows,
        sum(e.amount) as total_amount
    from transactions e
    join accounts dim on e.account_id = dim.id
    group by dim.status, dim.account_type, e.merchant
)
select
    status,
    account_type,
    merchant,
    total_amount
from dim_totals
order by status, account_type desc;
