with enriched as (
    -- Running and moving aggregates
    select
        id,
        promo_code,
        starts_on,
        ends_on,
        max(discount_pct) over (partition by promo_code order by ends_on, id rows between unbounded preceding and current row) as running_max_discount_pct,
        count(discount_pct) over (partition by promo_code order by ends_on, id rows between unbounded preceding and current row) as running_count_discount_pct,
        sum(discount_pct) over (partition by promo_code order by ends_on, id rows between unbounded preceding and current row) as running_sum_discount_pct,
        avg(discount_pct) over (partition by promo_code order by ends_on, id rows between unbounded preceding and current row) as running_avg_discount_pct
    from promotions
    where promo_code = 'VIP'
),
with_windows as (
    -- Window metrics over several different partitions
    select
        id,
        starts_on,
        running_max_discount_pct,
        running_sum_discount_pct,
        row_number() over (order by starts_on, id) as seq_no,
        avg(running_sum_discount_pct) over (order by running_max_discount_pct, id rows between 2 preceding and current row) as running_sum_discount_pct_avg_2,
        sum(running_avg_discount_pct) over w_ordered as running_running_avg_discount_pct
    from enriched
    window w_ordered as (partition by promo_code order by running_avg_discount_pct, id rows between unbounded preceding and current row)
),
scored as (
    -- Ranking and gap features
    select
        *,
        lag(running_max_discount_pct) over (order by starts_on, id) as prev_running_max_discount_pct,
        lead(running_sum_discount_pct) over (order by starts_on, id) as next_running_sum_discount_pct,
        row_number() over (order by starts_on, id) as seq_no_2
    from with_windows
),
cleaned as (
    -- Filter the raw rows and derive helper columns
    select
        id,
        starts_on,
        running_sum_discount_pct,
        seq_no,
        running_sum_discount_pct_avg_2,
        running_running_avg_discount_pct,
        prev_running_max_discount_pct,
        seq_no_2,
        greatest(seq_no_2, running_max_discount_pct) as greatest_seq_no_2_running_max_discount_pct
    from scored
    where starts_on >= date '2023-05-07'
)
-- Grouped metrics
select
    case when seq_no_2 >= 7 then 'HIGH' else 'LOW' end as seq_no_2_tier,
    date_trunc('week', starts_on) as week_start,
    avg(prev_running_max_discount_pct) as avg_prev_running_max_discount_pct,
    avg(prev_running_max_discount_pct) as avg_prev_running_max_discount_pct_2,
    count(distinct id) as distinct_ids,
    round(avg(seq_no), 2) as avg_seq_no,
    count(distinct id) as distinct_ids_2,
    count(distinct id) as distinct_ids_3,
    rank() over (partition by case when seq_no_2 >= 7 then 'HIGH' else 'LOW' end order by count(distinct id) desc) as distinct_ids_2_rank
from cleaned
group by case when seq_no_2 >= 7 then 'HIGH' else 'LOW' end, date_trunc('week', starts_on)
