with filtered as (
    -- Keep recent, valid rows
    select
        id,
        name,
        country,
        city,
        segment,
        signup_date,
        is_active,
        credit_limit,
        cast(is_active as int) as is_active_flag,
        coalesce(city, 'NONE') as resolved_city
    from customers
    where country in ('SG', 'US')
),
with_windows as (
    -- Running and moving aggregates
    select
        id,
        country,
        city,
        segment,
        is_active,
        is_active_flag,
        resolved_city,
        lag(is_active_flag) over (partition by city order by credit_limit, id) as prev_is_active_flag,
        lead(credit_limit) over (partition by city order by credit_limit, id) as next_credit_limit,
        lag(credit_limit) over (partition by city order by credit_limit, id) as prev_credit_limit,
        row_number() over (partition by city order by credit_limit, id) as seq_no
    from filtered
),
with_latest as (
    -- Latest matching account per row (LATERAL ... LIMIT 1)
    select
        s.id,
        s.country,
        s.city,
        s.segment,
        s.resolved_city,
        s.next_credit_limit,
        s.prev_credit_limit,
        s.seq_no,
        latest.balance
    from with_windows s
    left join lateral (
            select c.balance
            from accounts c
            where c.customer_id = s.id
            order by c.opened_date desc, c.id desc
            limit 1
        ) latest on true
),
enriched as (
    -- Ranking and gap features
    select
        *,
        min(balance) over w_recent as last7_min_balance,
        avg(balance) over w_recent as last7_avg_balance,
        max(balance) over w_recent as last7_max_balance,
        row_number() over (partition by country order by balance desc, id) as row_seq
    from with_latest
    window w_recent as (partition by city order by balance, id rows between 6 preceding and current row)
),
base as (
    -- Basic cleansing and defaults
    select
        id,
        country,
        segment,
        resolved_city,
        next_credit_limit,
        prev_credit_limit,
        seq_no,
        balance,
        last7_avg_balance,
        last7_max_balance,
        row_seq,
        row_seq + seq_no as row_seq_plus_seq_no,
        case when seq_no >= 8 then 'High' when seq_no >= 2 then 'Medium' else 'Low' end as seq_no_level,
        coalesce(segment, 'N/A') as segment_clean
    from enriched
    where city is not null
)
select
    id,
    country,
    segment,
    next_credit_limit,
    prev_credit_limit,
    seq_no,
    row_seq,
    row_seq_plus_seq_no
from base;
