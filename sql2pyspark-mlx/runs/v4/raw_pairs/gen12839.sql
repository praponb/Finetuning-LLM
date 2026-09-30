with cleaned as (
    -- Keep recent, valid rows
    select
        id,
        customer_id,
        account_type,
        opened_date,
        status,
        coalesce(status, 'Unknown') as status_filled
    from accounts
    where opened_date > date '2024-02-02'
      and status is not null
)
-- Latest matching transaction per row (LATERAL ... LIMIT 1)
select s.*, top1.merchant
from cleaned s
join lateral (
        select c.merchant
        from transactions c
        where c.account_id = s.id
          and c.txn_time <= s.opened_date
        order by c.txn_time desc, c.id desc
        limit 1
    ) top1 on true;
