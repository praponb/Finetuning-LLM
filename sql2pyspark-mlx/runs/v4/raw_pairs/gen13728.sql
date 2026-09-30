with base as (
    -- Filter the raw rows and derive helper columns
    select
        id,
        promo_code,
        discount_pct,
        date_format(starts_on, 'yyyy-MM') as starts_on_label
    from promotions
    where starts_on > date '2023-10-11'
      and channel is not null
      and month(starts_on) in (1, 3, 6)
),
deduped as (
    -- QUALIFY-style window filter
    select *, row_number() over (partition by promo_code order by discount_pct, id) as rn
    from base
    qualify rn = 1
),
latest as (
    -- Keep one row per key (latest first)
    select *
    from deduped
    qualify rank() over (partition by promo_code order by discount_pct desc) = 1
),
base_promotions as (
    -- Keep recent, valid rows
    select
        id,
        promo_code,
        discount_pct,
        starts_on_label,
        rn,
        floor(discount_pct) as discount_pct_whole,
        cast(discount_pct as int) as discount_pct_int
    from latest
    where discount_pct > 0.21
),
base_promotions_2 as (
    -- Filter the raw rows and derive helper columns
    select *, upper(promo_code) as promo_code_upper
    from base_promotions
    where discount_pct >= 0.19
),
with_windows as (
    -- Ranking and gap features
    select
        *,
        sum(discount_pct_int) over (partition by promo_code order by discount_pct, id rows between unbounded preceding and current row) as running_discount_pct_int,
        row_number() over (partition by promo_code order by discount_pct_whole desc, id) as row_seq,
        lead(discount_pct_whole, 1) over (order by discount_pct_int, id) as next_discount_pct_whole
    from base_promotions_2
)
-- Grouped metrics
select
    case when running_discount_pct_int >= 442 then 'LARGE' else 'SMALL' end as running_discount_pct_int_tier,
    stddev_samp(discount_pct) as discount_pct_stddev,
    count(*) as num_rows,
    round(avg(next_discount_pct_whole), 2) as avg_next_discount_pct_whole,
    approx_percentile(row_seq, array(0.25, 0.5, 0.75)) as row_seq_quartiles
from with_windows
group by case when running_discount_pct_int >= 442 then 'LARGE' else 'SMALL' end
order by num_rows nulls first;
