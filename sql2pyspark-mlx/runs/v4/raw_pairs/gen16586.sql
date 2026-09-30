with cleaned as (
    -- Basic cleansing and defaults
    select
        *,
        trim(email) as clean_email,
        trim(city) as clean_city,
        coalesce(segment, 'NONE') as segment_filled
    from customers
    where signup_date >= date '2024-01-28'
),
latest as (
    -- QUALIFY-style window filter
    select *
    from (
        select *, rank() over (partition by city order by age desc) as rn
        from cleaned
    ) ranked
    where rn = 1
),
with_last as (
    -- Latest matching account per row (LATERAL ... LIMIT 1)
    select
        s.*,
        latest.account_account_type,
        latest.account_status
    from latest s
    left join lateral (
            select c.account_type as account_account_type, c.status as account_status
            from accounts c
            where c.customer_id = s.id
            order by c.opened_date desc, c.id desc
            limit 1
        ) latest on true
),
deduped as (
    -- Deduplicate with ROW_NUMBER
    select *
    from (
        select *, row_number() over (partition by account_status order by rn, id) as rn_2
        from with_last
    ) x
    where rn_2 = 1
),
ranked as (
    -- Keep one row per key (latest first)
    select *
    from deduped
    qualify row_number() over (partition by city, account_status order by rn_2 desc, id) = 1
),
with_windows as (
    -- Running and moving aggregates
    select
        id,
        name,
        email,
        country,
        city,
        segment,
        signup_date,
        is_active,
        credit_limit,
        age,
        clean_email,
        clean_city,
        segment_filled,
        rn,
        account_account_type,
        account_status,
        rn_2,
        ntile(3) over (partition by account_account_type order by age, id) as age_bucket,
        cume_dist() over (partition by city order by credit_limit desc) as credit_limit_cume_dist
    from ranked
)
-- Window metrics over several different partitions
select
    *,
    rank() over w_grp as credit_limit_rank,
    row_number() over (partition by city order by rn_2, id) as row_seq,
    sum(age_bucket) over (partition by account_account_type order by rn_2, id rows between unbounded preceding and current row) as cumulative_age_bucket
from with_windows
window w_grp as (order by credit_limit nulls last);
