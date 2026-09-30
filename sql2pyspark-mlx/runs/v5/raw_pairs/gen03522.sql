with base as (
    -- Filter the raw rows and derive helper columns
    select *, case when balance >= 87000 then 'High' when balance >= 15000 then 'Medium' else 'Low' end as balance_tier
    from accounts
    where opened_date > to_date('2023-03-19')
      and customer_id is null
),
with_dims as (
    -- Attach customers attributes
    select a.*, b.name
    from base a
    left join customers b
        on a.customer_id = b.id
),
with_windows as (
    -- Window metrics over several different partitions
    select
        *,
        max(balance) over (partition by customer_id order by opened_date, id rows between 6 preceding and current row) as last7_max_balance,
        sum(balance) over (partition by customer_id order by opened_date, id rows between 6 preceding and current row) as last7_sum_balance,
        count(balance) over (partition by customer_id order by opened_date, id rows between 6 preceding and current row) as last7_count_balance,
        avg(balance) over (partition by customer_id order by opened_date, id rows between 6 preceding and current row) as last7_avg_balance
    from with_dims
),
enriched as (
    -- Attach customers attributes
    select
        e.id,
        e.customer_id,
        e.account_type,
        e.opened_date,
        e.name,
        e.last7_max_balance,
        e.last7_sum_balance,
        e.last7_count_balance,
        e.last7_avg_balance,
        c.email
    from with_windows e
    left join customers c
        on e.customer_id <=> c.id
),
matched_keys as (
    -- Set difference on keys
    select customer_id, id
    from enriched
    except all
    select customer_id, id
    from accounts
    where account_type = 'CREDIT'
),
kept_rows as (
    -- Keep rows whose keys survive the set operation
    select s.*
    from enriched s
    inner join matched_keys k on s.customer_id = k.customer_id and s.id = k.id
),
stats as (
    -- Grouped metrics
    select
        date_trunc('quarter', opened_date) as quarter_start,
        date_trunc('week', opened_date) as week_start,
        sort_array(collect_set(account_type)) as account_type_list,
        round(avg(last7_sum_balance), 2) as avg_last7_sum_balance,
        sort_array(collect_set(account_type)) as account_type_list_2
    from kept_rows
    group by date_trunc('quarter', opened_date), date_trunc('week', opened_date)
    having round(avg(last7_sum_balance), 2) > 0
)
select
    quarter_start,
    week_start,
    account_type_list,
    avg_last7_sum_balance,
    account_type_list_2
from stats
