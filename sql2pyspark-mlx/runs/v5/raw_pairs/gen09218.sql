with cleaned as (
    -- Keep recent, valid rows
    select *, cast(is_active as int) as is_active_flag
    from customers
    where segment is distinct from 'SMB'
      and signup_date > date '2023-07-08'
),
ranked as (
    -- Keep one row per key (latest first)
    select *, row_number() over (partition by city order by is_active_flag desc, id) as rank_in_group
    from cleaned
    qualify rank_in_group = 1
),
ranked_customers as (
    -- Keep one row per key (latest first)
    select *, row_number() over (partition by city order by signup_date, id) as rank_in_group_2
    from ranked
    qualify rank_in_group_2 = 1
),
scored as (
    -- Ranking and gap features
    select
        *,
        first_value(is_active_flag, true) over w_ordered as first_known_is_active_flag,
        rank() over w_ordered_2 as is_active_flag_rank,
        percent_rank() over (partition by country order by credit_limit desc nulls first) as credit_limit_percent_rank,
        sum(is_active_flag) over (order by is_active_flag, id rows between unbounded preceding and current row) as cumulative_is_active_flag,
        round(credit_limit / sum(credit_limit) over (partition by city), 4) as credit_limit_share
    from ranked_customers
    where credit_limit < 8100
    window w_ordered as (partition by country order by is_active_flag, id rows between unbounded preceding and current row), w_ordered_2 as (partition by segment order by is_active_flag nulls last)
),
kept as (
    -- Semi-join via a correlated subquery
    select s.*
    from scored s
    where exists (
            select 1
            from accounts x
            where x.customer_id = s.id
              and x.opened_date <= '2023-12-01'
        )
)
select
    id,
    name,
    signup_date,
    is_active,
    credit_limit,
    age,
    is_active_flag,
    rank_in_group,
    cumulative_is_active_flag
from kept
order by credit_limit desc nulls last
