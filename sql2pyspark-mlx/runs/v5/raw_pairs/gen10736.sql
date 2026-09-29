with metrics as (
    select
        id,
        city,
        region,
        opened_date,
        sqft,
        row_number() over (order by opened_date desc, id) as seq_no,
        sum(sqft) over (partition by city order by sqft, id rows between unbounded preceding and current row) as running_sqft,
        first_value(region, true) over (partition by city order by opened_date, id rows between unbounded preceding and current row) as first_known_region,
        avg(sqft) over (partition by city order by sqft, id rows between 6 preceding and 1 preceding) as sqft_avg_6_prior
    from stores
),
with_windows as (
    select
        id,
        city,
        opened_date,
        sqft,
        seq_no,
        sqft_avg_6_prior,
        row_number() over (partition by city order by sqft_avg_6_prior, id) as event_seq,
        lag(seq_no, 2) over w_grp as prev_seq_no
    from metrics
    window w_grp as (partition by city, region order by seq_no, id)
),
scored as (
    select
        id,
        event_seq,
        prev_seq_no,
        count(sqft) over w_part as running_count_sqft,
        min(sqft) over w_part as running_min_sqft,
        max(sqft) over w_part as running_max_sqft,
        last_value(sqft, true) over w_part as last_known_sqft
    from with_windows
    where sqft_avg_6_prior between 9639 and 10943
    window w_part as (partition by city order by event_seq, id rows between unbounded preceding and current row)
),
filtered as (
    select
        *,
        running_count_sqft + prev_seq_no as running_count_sqft_plus_prev_seq_no,
        cast(running_max_sqft as int) as running_max_sqft_int
    from scored
    where prev_seq_no is not null
)
select
    case when event_seq >= 7 then 'PREMIUM' else 'STANDARD' end as event_seq_tier,
    case when running_count_sqft >= 15 then 'PREMIUM' else 'STANDARD' end as running_count_sqft_tier,
    case when running_max_sqft_int >= 63 then 'LARGE' else 'SMALL' end as running_max_sqft_int_tier,
    percentile_approx(running_max_sqft, array(0.25, 0.5, 0.75)) as running_max_sqft_quartiles,
    round(avg(running_min_sqft), 2) as avg_running_min_sqft
from filtered
group by case when event_seq >= 7 then 'PREMIUM' else 'STANDARD' end, case when running_count_sqft >= 15 then 'PREMIUM' else 'STANDARD' end, case when running_max_sqft_int >= 63 then 'LARGE' else 'SMALL' end;
