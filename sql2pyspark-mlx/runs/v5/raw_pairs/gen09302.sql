with cleaned as (
    select
        *,
        cast(is_flagged as int) as is_flagged_flag,
        coalesce(merchant, 'NONE') as merchant_clean
    from transactions
    where merchant is not null
)
select
    p.account_type,
    a.merchant,
    count(*) as rows_total,
    sum(a.amount) as total_amount,
    grouping_id() as level_id
from cleaned a
inner join accounts p on a.account_id = p.id
group by cube (p.account_type, a.merchant)
order by rows_total nulls last
