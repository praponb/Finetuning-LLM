with scored as (
    -- Ranking and gap features
    select
        *,
        round(monthly_fee / sum(monthly_fee) over (partition by status), 4) monthly_fee_share,
        first_value(monthly_fee) ignore nulls over (partition by status order by monthly_fee, id rows between unbounded preceding and current row) first_known_monthly_fee,
        sum(monthly_fee) over (partition by status order by monthly_fee, id rows between unbounded preceding and current row) running_monthly_fee
    from subscriptions
    where monthly_fee is not null
),
joined as (
    -- Attach customers attributes
    select
        e.*,
        d.city,
        d.name
    from scored e
    inner join customers d
        on e.customer_id = d.id
),
latest as (
    -- QUALIFY-style window filter
    select *
    from (
        select *, row_number() over (partition by status order by end_date nulls last, id) as rn
        from joined
    ) t
    where rn = 1
),
with_dims as (
    -- Attach customers attributes
    select
        src.*,
        p.credit_limit,
        p.email,
        p.city as customer_city
    from latest src
    left join customers p
        on src.customer_id = p.id
),
filtered as (
    -- Keep recent, valid rows
    select
        id,
        customer_id,
        start_date,
        monthly_fee,
        status,
        monthly_fee_share,
        city,
        rn,
        credit_limit,
        email,
        date_format(start_date, 'yyyy-MM-dd') start_date_label
    from with_dims
    where end_date > date '2023-11-02'
      and city is not null
),
stats as (
    -- Multi-level aggregation
    select
        date_trunc('week', start_date) as week_start,
        round(avg(monthly_fee_share), 2) as avg_monthly_fee_share,
        count(case when city = 'Chiang Mai' then 1 end) as chiang_mai_count,
        avg(credit_limit) filter (where city = 'Singapore') as singapore_avg_credit_limit,
        approx_percentile(credit_limit, 0.5) as credit_limit_p50,
        percentile_approx(monthly_fee, 0.9) as monthly_fee_p90
    from filtered
    group by date_trunc('week', start_date)
)
select
    week_start,
    avg_monthly_fee_share,
    chiang_mai_count,
    singapore_avg_credit_limit,
    credit_limit_p50,
    monthly_fee_p90
from stats
order by monthly_fee_p90 desc nulls last;
