with cleaned as (
    -- Filter the raw rows and derive helper columns
    select *, case when status in ('CLOSED', 'FROZEN') then 'Tier 1' else 'Other' end as status_group
    from accounts
    where opened_date < '2024-02-19'
),
joined as (
    -- Attach customers attributes
    select
        s.*,
        b.credit_limit,
        b.city,
        b.signup_date
    from cleaned s
    join customers b
        on s.customer_id = b.id
),
deduped as (
    -- Deduplicate with ROW_NUMBER
    select *, row_number() over (partition by account_type order by signup_date desc, id asc) as dedup_rank
    from joined
    qualify dedup_rank = 1
),
ranked as (
    -- Keep one row per key (latest first)
    select *
    from deduped
    qualify row_number() over (partition by city, account_type order by signup_date desc nulls first, id asc) = 1
)
-- Multi-level aggregation
select
    status,
    size(collect_set(account_type)) as num_account_types,
    size(collect_set(city)) as num_cities,
    percentile_approx(balance, array(0.25, 0.5, 0.75)) as balance_quartiles,
    percentile_approx(credit_limit, array(0.25, 0.5, 0.75)) as credit_limit_quartiles
from ranked
group by status;
