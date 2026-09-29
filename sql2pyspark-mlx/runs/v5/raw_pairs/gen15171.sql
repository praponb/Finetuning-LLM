with filtered as (
    -- Basic cleansing and defaults
    select
        id,
        last_restock,
        date_add(last_restock, 57) as last_restock_plus_57d,
        weekofyear(last_restock) as last_restock_weekofyear
    from inventory
    where last_restock >= to_date('2023-08-29')
),
cleaned as (
    -- Filter the raw rows and derive helper columns
    select *, datediff(last_restock, last_restock_plus_57d) as days_last_restock_plus_57d_to_last_restock
    from filtered
    where last_restock_weekofyear between 43 and 76
),
enriched as (
    -- Running and moving aggregates
    select
        *,
        sum(last_restock_weekofyear) over w_running as running_last_restock_weekofyear,
        avg(days_last_restock_plus_57d_to_last_restock) over (order by last_restock_plus_57d, id rows between 6 preceding and current row) as days_last_restock_plus_57d_to_last_restock_avg_6,
        avg(last_restock_weekofyear) over (order by last_restock, id rows between 5 preceding and 1 preceding) as last_restock_weekofyear_avg_5_prior,
        sum(last_restock_weekofyear) over w_running as cumulative_last_restock_weekofyear
    from cleaned
    where year(last_restock_plus_57d) >= 2025
    window w_running as (order by last_restock_plus_57d, id rows between unbounded preceding and current row)
),
with_windows as (
    -- Ranking and gap features
    select
        *,
        row_number() over (order by days_last_restock_plus_57d_to_last_restock_avg_6, id) as row_seq,
        lead(last_restock_weekofyear_avg_5_prior, 1) over (order by days_last_restock_plus_57d_to_last_restock_avg_6, id) as next_last_restock_weekofyear_avg_5_prior,
        sum(last_restock_weekofyear) over (order by last_restock_weekofyear_avg_5_prior, id rows between unbounded preceding and current row) as running_last_restock_weekofyear_2,
        cume_dist() over (order by cumulative_last_restock_weekofyear) as cumulative_last_restock_weekofyear_cume_dist
    from enriched
),
enriched_inventory as (
    -- Ranking and gap features
    select
        *,
        lag(running_last_restock_weekofyear_2) over w_part as prev_running_last_restock_weekofyear_2,
        lag(last_restock_weekofyear) over w_part as prev_last_restock_weekofyear,
        row_number() over w_part as row_seq_2
    from with_windows
    window w_part as (order by running_last_restock_weekofyear_2, id)
),
enriched_inventory_2 as (
    -- Ranking and gap features
    select
        id,
        last_restock_plus_57d,
        days_last_restock_plus_57d_to_last_restock_avg_6,
        last_restock_weekofyear_avg_5_prior,
        cumulative_last_restock_weekofyear,
        prev_running_last_restock_weekofyear_2,
        prev_last_restock_weekofyear,
        avg(row_seq) over (order by row_seq, id rows between 3 preceding and current row) as last4_avg_row_seq,
        count(row_seq) over (order by row_seq, id rows between 3 preceding and current row) as last4_count_row_seq,
        max(row_seq) over (order by row_seq, id rows between 3 preceding and current row) as last4_max_row_seq,
        min(row_seq) over (order by row_seq, id rows between 3 preceding and current row) as last4_min_row_seq,
        last_value(cumulative_last_restock_weekofyear_cume_dist, true) over (order by next_last_restock_weekofyear_avg_5_prior, id rows between unbounded preceding and current row) as last_known_cumulative_last_restock_weekofyear_cume_dist
    from enriched_inventory
),
grouped as (
    -- Grouped metrics
    select
        date_trunc('month', last_restock_plus_57d) as activity_month,
        case when days_last_restock_plus_57d_to_last_restock_avg_6 >= 70 then 'PREMIUM' else 'STANDARD' end as days_last_restock_plus_57d_to_last_restock_avg_6_tier,
        approx_percentile(days_last_restock_plus_57d_to_last_restock_avg_6, 0.95) as days_last_restock_plus_57d_to_last_restock_avg_6_p95,
        round(avg(last4_avg_row_seq), 2) as avg_last4_avg_row_seq,
        percentile_approx(prev_last_restock_weekofyear, 0.95) as prev_last_restock_weekofyear_p95,
        count(distinct id) as distinct_ids,
        grouping_id() as gid
    from enriched_inventory_2
    group by cube (date_trunc('month', last_restock_plus_57d), case when days_last_restock_plus_57d_to_last_restock_avg_6 >= 70 then 'PREMIUM' else 'STANDARD' end)
    having count(distinct id) > 1
)
select
    days_last_restock_plus_57d_to_last_restock_avg_6_p95,
    avg_last4_avg_row_seq,
    prev_last_restock_weekofyear_p95,
    distinct_ids
from grouped
order by avg_last4_avg_row_seq desc nulls first
