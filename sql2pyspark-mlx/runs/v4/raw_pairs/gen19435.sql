with base as (
    -- Basic cleansing and defaults
    select
        id,
        customer_id,
        balance,
        opened_date,
        status,
        trim(account_type) as clean_account_type,
        coalesce(status, 'UNKNOWN') as resolved_status
    from accounts
    where opened_date >= date '2023-08-20'
      and month(opened_date) in (2, 9, 12)
),
metrics as (
    -- Window metrics over several different partitions
    select
        *,
        percent_rank() over (partition by customer_id, status order by balance desc nulls last) as balance_percent_rank,
        sum(balance) over w_recent as running_balance,
        count(balance) over (partition by customer_id) as customer_id_count_balance,
        row_number() over (partition by status order by opened_date desc, id) as row_seq,
        row_number() over w_part as seq_no
    from base
    window w_recent as (partition by status order by balance, id rows between unbounded preceding and current row), w_part as (partition by customer_id order by balance, id)
),
enriched as (
    -- Attach customers attributes
    select
        src.*,
        d.city,
        d.credit_limit,
        d.signup_date
    from metrics src
    inner join customers d
        on src.customer_id = d.id
),
ranked as (
    -- QUALIFY-style window filter
    select *, row_number() over (partition by city, status order by signup_date nulls last, id) as row_num
    from enriched
    qualify row_num = 1
),
totals as (
    -- Aggregate with conditional metrics
    select
        case when customer_id_count_balance >= 16 then 'LARGE' else 'SMALL' end as customer_id_count_balance_tier,
        city,
        round(sum(balance_percent_rank) / nullif(count(case when status = 'CLOSED' then 1 end), 0), 2) as balance_percent_rank_per_closed,
        count_if(city = 'Tokyo') as tokyo_events,
        count(*) as cnt,
        sum(case when status = 'FROZEN' then balance else 0 end) as frozen_balance,
        sum(case when city = 'Bangkok' then running_balance else 0 end) as bangkok_running_balance,
        sum(case when city = 'New York' then credit_limit else 0 end) as new_york_credit_limit
    from ranked
    group by cube (case when customer_id_count_balance >= 16 then 'LARGE' else 'SMALL' end, city)
)
select
    balance_percent_rank_per_closed,
    cnt,
    frozen_balance,
    bangkok_running_balance,
    new_york_credit_limit
from totals
order by new_york_credit_limit desc nulls first;
