with latest_match as (
    -- Latest matching transaction per row (LATERAL ... LIMIT 1)
    select
        s.id,
        s.customer_id,
        s.account_type,
        s.status,
        x.merchant
    from accounts s
    left join lateral (
            select c.merchant
            from transactions c
            where c.account_id = s.id
              and c.txn_time <= s.opened_date
            order by c.txn_time desc, c.id desc
            limit 1
        ) x on true
)
-- Aggregate with conditional metrics
select
    account_type,
    count(*) as cnt,
    size(collect_set(merchant)) as num_merchants,
    count_if(status = 'CLOSED') as closed_events,
    sort_array(collect_set(merchant)) as merchant_list,
    count(case when merchant = 'Starbucks' then 1 end) as starbucks_count
from latest_match
group by account_type
having starbucks_count > 1
order by account_type
