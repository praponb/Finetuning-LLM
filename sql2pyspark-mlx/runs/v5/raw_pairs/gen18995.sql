with cleaned as (
    -- Filter the raw rows and derive helper columns
    select
        id,
        channel,
        promo_code,
        starts_on,
        ends_on,
        discount_pct,
        cast(discount_pct as int) as discount_pct_int
    from promotions
    where ends_on > to_date('2023-06-11')
      and ends_on > '2024-09-19'
),
filtered as (
    -- Keep recent, valid rows
    select
        *,
        upper(promo_code) as promo_code_upper,
        ceil(discount_pct_int) as discount_pct_int_whole
    from cleaned
    where starts_on >= to_date('2023-04-02')
      and channel is not null
),
with_windows as (
    -- Running and moving aggregates
    select
        *,
        first_value(channel, true) over (partition by channel order by discount_pct, id rows between unbounded preceding and current row) as first_known_channel,
        lead(discount_pct_int, 1) over (partition by promo_code, channel order by discount_pct_int, id) as next_discount_pct_int,
        row_number() over (partition by channel order by ends_on, id) as seq_no
    from filtered
),
metrics as (
    -- Ranking and gap features
    select
        *,
        lead(seq_no) over (partition by promo_code order by discount_pct_int_whole, id) as next_seq_no,
        lag(discount_pct_int_whole) over (partition by promo_code order by discount_pct_int_whole, id) as prev_discount_pct_int_whole,
        lead(discount_pct_int_whole) over (partition by promo_code order by discount_pct_int_whole, id) as next_discount_pct_int_whole,
        row_number() over (partition by promo_code order by discount_pct_int_whole, id) as row_seq,
        row_number() over (partition by promo_code order by discount_pct, id) as event_seq
    from with_windows
),
attributed as (
    -- Non-equi join on a validity window
    select
        a.*,
        pr.id as promo_id,
        pr.promo_code as promotion_promo_code,
        pr.discount_pct as promotion_discount_pct
    from metrics a
    left join promotions pr
        on a.channel <=> pr.channel
       and a.ends_on between pr.starts_on and pr.ends_on
)
-- Aggregate with conditional metrics
select
    case when prev_discount_pct_int_whole >= 69 then 'LARGE' else 'SMALL' end as prev_discount_pct_int_whole_tier,
    date_trunc('month', starts_on) as activity_month,
    sort_array(collect_set(channel)) as channel_list,
    approx_percentile(row_seq, array(0.25, 0.5, 0.75)) as row_seq_quartiles,
    round(avg(event_seq), 2) as avg_event_seq,
    sum(promotion_discount_pct) filter (where channel = 'web') as web_total_promotion_discount_pct
from attributed
group by grouping sets ((case when prev_discount_pct_int_whole >= 69 then 'LARGE' else 'SMALL' end, date_trunc('month', starts_on)), (case when prev_discount_pct_int_whole >= 69 then 'LARGE' else 'SMALL' end))
having round(avg(event_seq), 2) > 0
order by activity_month desc
