with enriched as (
    -- Ranking and gap features
    select
        *,
        dense_rank() over (partition by region order by sqft asc) as sqft_dense_rank,
        row_number() over (partition by region order by opened_date asc, id asc) as seq_no,
        lead(sqft, 1) over (order by opened_date asc, id asc) as next_sqft
    from stores
)
-- Multi-level aggregation
select
    date_trunc('month', opened_date) as activity_month,
    city,
    count(case when city = 'Bangkok' then 1 end) as bangkok_count,
    bit_and(sqft) as sqft_bit_and,
    bit_or(sqft) as sqft_bit_or,
    approx_percentile(sqft, array(0.25, 0.5, 0.75)) as sqft_quartiles,
    count(*) as event_count,
    sum(case when city = 'Singapore' then sqft else 0 end) as singapore_sqft,
    grouping_id() as grouping_level
from enriched
group by grouping sets ((date_trunc('month', opened_date), city), (date_trunc('month', opened_date)), ())
order by activity_month desc nulls last
