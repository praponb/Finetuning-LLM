with metrics as (
    -- Ranking and gap features
    select
        *,
        round(sqft / sum(sqft) over (partition by city), 4) as sqft_share,
        rank() over (partition by city order by sqft asc nulls first) as sqft_rank,
        avg(sqft) over (partition by city order by sqft asc, id asc rows between 3 preceding and 1 preceding) as sqft_avg_3_prior,
        row_number() over w_ordered as row_seq
    from stores
    window w_ordered as (partition by city, region order by opened_date desc, id asc)
),
enriched as (
    -- Running and moving aggregates
    select
        *,
        ntile(5) over (partition by region order by row_seq desc, id asc) as row_seq_bucket,
        sum(sqft_share) over (order by row_seq asc, id asc rows between unbounded preceding and current row) as cumulative_sqft_share,
        cume_dist() over w_grp as sqft_avg_3_prior_cume_dist
    from metrics
    window w_grp as (partition by city order by sqft_avg_3_prior asc nulls last)
),
metrics_stores as (
    -- Ranking and gap features
    select
        id,
        store_name,
        city,
        region,
        opened_date,
        sqft,
        sqft_share,
        sqft_rank,
        sqft_avg_3_prior,
        row_seq,
        row_seq_bucket,
        cumulative_sqft_share,
        sqft_avg_3_prior_cume_dist,
        stddev_samp(sqft_avg_3_prior_cume_dist) over (partition by city, region) as city_stddev_samp_sqft_avg_3_prior_cume_dist,
        lead(sqft_avg_3_prior_cume_dist, 1) over (partition by city order by sqft_share asc, id asc) as next_sqft_avg_3_prior_cume_dist,
        sum(sqft) over (order by sqft_share asc, id asc rows between unbounded preceding and current row) as cumulative_sqft
    from enriched
)
select
    id,
    store_name,
    region,
    sqft,
    sqft_share,
    row_seq,
    row_seq_bucket,
    cumulative_sqft_share,
    sqft_avg_3_prior_cume_dist
from metrics_stores
order by sqft_avg_3_prior_cume_dist asc nulls last;
