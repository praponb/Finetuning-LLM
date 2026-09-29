with valid as (
    -- Filter the raw rows and derive helper columns
    select
        id,
        channel,
        starts_on,
        ends_on,
        length(promo_code) promo_code_length
    from promotions
    where starts_on >= date '2023-09-29'
      and year(ends_on) = 2025
),
deduped as (
    -- QUALIFY-style window filter
    select
        id,
        starts_on,
        ends_on,
        promo_code_length
    from (
        select *, row_number() over (partition by channel order by ends_on asc, id asc) as rank_in_group
        from valid
    ) t
    where rank_in_group = 1
),
base as (
    -- Keep recent, valid rows
    select
        *,
        date_format(ends_on, 'yyyy-MM-dd') ends_on_label,
        date_format(starts_on, 'yyyy-MM-dd') starts_on_label
    from deduped
    where starts_on > date '2023-07-29'
),
with_windows as (
    -- Ranking and gap features
    select
        *,
        sum(promo_code_length) over (order by ends_on asc, id asc rows between unbounded preceding and current row) running_promo_code_length,
        ntile(4) over (order by promo_code_length asc, id asc) promo_code_length_bucket,
        row_number() over (order by starts_on asc, id asc) row_seq,
        lag(promo_code_length, 1) over (order by ends_on asc, id asc) prev_promo_code_length,
        cume_dist() over (order by promo_code_length asc nulls first) promo_code_length_cume_dist
    from base
    where starts_on between '2024-08-20' and '2025-01-01'
),
filtered as (
    -- Filter the raw rows and derive helper columns
    select *, promo_code_length_bucket * promo_code_length promo_code_length_bucket_times_promo_code_length
    from with_windows
    where starts_on > to_date('2023-08-07')
)
select
    id,
    starts_on,
    ends_on,
    promo_code_length,
    ends_on_label,
    row_seq,
    prev_promo_code_length,
    promo_code_length_cume_dist,
    promo_code_length_bucket_times_promo_code_length
from filtered
order by ends_on desc nulls first
