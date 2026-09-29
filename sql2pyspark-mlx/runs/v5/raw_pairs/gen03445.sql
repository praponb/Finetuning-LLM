with joined as (
    -- Attach stores attributes
    select e.*, b.region
    from inventory e
    join stores b
        on e.store_id = b.id
),
with_dims as (
    -- Attach stores attributes
    select
        s.id,
        s.store_id,
        s.last_restock,
        s.region,
        d.sqft,
        d.region as store_region
    from joined s
    left join stores d
        on s.store_id = d.id
    where d.region not in ('Central', 'South')
),
scored as (
    -- Ranking and gap features
    select
        id,
        sqft,
        row_number() over (partition by region order by last_restock, id) as event_seq,
        lag(sqft, 1, 0) over (partition by store_region order by sqft, id) as prev_sqft,
        avg(sqft) over (partition by store_region, region order by last_restock, id rows between 3 preceding and current row) as sqft_avg_3,
        last_value(sqft) ignore nulls over (partition by region order by sqft, id rows between unbounded preceding and current row) as last_known_sqft
    from with_dims
),
cleaned as (
    -- Filter the raw rows and derive helper columns
    select
        id,
        sqft,
        event_seq,
        prev_sqft,
        sqft_avg_3,
        last_known_sqft,
        sqft_avg_3 * event_seq as sqft_avg_3_times_event_seq,
        least(last_known_sqft, sqft) as least_last_known_sqft_sqft
    from scored
    where sqft between 9538 and 14236
),
valid as (
    -- Basic cleansing and defaults
    select
        *,
        greatest(sqft, sqft_avg_3) as greatest_sqft_sqft_avg_3,
        round(event_seq * 1.07, 2) as adjusted_event_seq
    from cleaned
    where last_known_sqft < 9887
)
-- Aggregate with conditional metrics
select
    case when sqft >= 14476 then 'PREMIUM' else 'STANDARD' end as sqft_tier,
    case when sqft_avg_3 >= 16122 then 'LARGE' else 'SMALL' end as sqft_avg_3_tier,
    percentile_approx(sqft_avg_3, array(0.25, 0.5, 0.75)) as sqft_avg_3_quartiles,
    percentile_approx(sqft, array(0.25, 0.5, 0.75)) as sqft_quartiles
from valid
group by case when sqft >= 14476 then 'PREMIUM' else 'STANDARD' end, case when sqft_avg_3 >= 16122 then 'LARGE' else 'SMALL' end
