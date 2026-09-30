with valid as (
    -- Filter the raw rows and derive helper columns
    select
        *,
        round(balance * 100, 2) as balance_adjusted,
        coalesce(status, 'unassigned') as status_filled
    from accounts
    where status is distinct from 'FROZEN'
      and customer_id is not null
),
with_windows as (
    -- Ranking and gap features
    select
        *,
        last_value(balance_adjusted, true) over w_running as last_known_balance_adjusted,
        dense_rank() over (partition by account_type order by balance_adjusted desc nulls last) as balance_adjusted_dense_rank,
        sum(balance) over (partition by status order by balance_adjusted, id rows between unbounded preceding and current row) as cumulative_balance
    from valid
    window w_running as (partition by status, customer_id order by balance, id rows between unbounded preceding and current row)
),
cleaned as (
    -- Filter the raw rows and derive helper columns
    select
        id,
        customer_id,
        balance,
        opened_date,
        status,
        status_filled,
        last_known_balance_adjusted,
        cumulative_balance,
        initcap(trim(status)) as status_title
    from with_windows
    where opened_date >= to_date('2023-08-19')
),
scored as (
    -- Window metrics over several different partitions
    select
        id,
        customer_id,
        opened_date,
        status,
        cumulative_balance,
        status_title,
        row_number() over (order by last_known_balance_adjusted, id) as row_seq,
        last_value(status, true) over (partition by customer_id order by cumulative_balance, id rows between unbounded preceding and current row) as last_known_status,
        count(cumulative_balance) over (partition by status) as status_count_cumulative_balance
    from cleaned
    where cumulative_balance >= 181787
)
-- Join a dimension and aggregate
select
    d.segment,
    d.country,
    count(*) as cnt,
    min(e.row_seq) as min_row_seq,
    sum(e.cumulative_balance) as total_cumulative_balance
from scored e
left join customers d on e.customer_id = d.id
group by rollup (d.segment, d.country)
order by total_cumulative_balance
