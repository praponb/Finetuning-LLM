with filtered as (
    select *, ceil(balance) as balance_whole
    from accounts
    where opened_date > '2024-04-01'
),
ranked as (
    select *, rank() over (partition by status, account_type order by balance desc) as rank_in_group
    from filtered
    qualify rank_in_group = 1
),
eligible as (
    select s.*
    from ranked s
    where exists (
            select 1
            from transactions ch
            where ch.account_id = s.id
              and ch.account_id in (15, 22)
        )
),
with_windows as (
    select
        id,
        customer_id,
        account_type,
        balance,
        opened_date,
        status,
        avg(balance_whole) over (order by balance, id rows between 6 preceding and current row) as balance_whole_avg_6,
        lead(balance_whole, 2) over w_running as next_balance_whole,
        ntile(4) over (partition by customer_id, status order by balance, id) as balance_bucket,
        sum(balance_whole) over (partition by status) as status_sum_balance_whole,
        percent_rank() over (order by balance desc) as balance_percent_rank
    from eligible
    where balance_whole = 28
    window w_running as (partition by customer_id order by balance_whole, id)
),
enriched as (
    select
        id,
        account_type,
        opened_date,
        status,
        next_balance_whole,
        balance_bucket,
        status_sum_balance_whole,
        balance_percent_rank,
        avg(balance_bucket) over (partition by customer_id, account_type order by balance_bucket, id rows between 3 preceding and current row) as balance_bucket_avg_3,
        avg(balance) over (partition by customer_id order by balance_bucket, id rows between 3 preceding and current row) as balance_avg_3
    from with_windows
),
summary as (
    select
        case when next_balance_whole >= 71 then 'LARGE' else 'SMALL' end as next_balance_whole_tier,
        count_if(account_type = 'CHECKING') as checking_events,
        round(avg(balance_bucket_avg_3), 2) as avg_balance_bucket_avg_3,
        avg(balance_avg_3) filter (where status = 'FROZEN') as frozen_avg_balance_avg_3
    from enriched
    group by case when next_balance_whole >= 71 then 'LARGE' else 'SMALL' end
)
select next_balance_whole_tier, avg_balance_bucket_avg_3
from summary
order by avg_balance_bucket_avg_3 nulls first, next_balance_whole_tier
limit 10;
