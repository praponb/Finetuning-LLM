with enriched as (
    select
        id,
        email,
        segment,
        is_active,
        count(credit_limit) over (partition by country) as country_count_credit_limit,
        ntile(4) over (partition by country order by credit_limit, id) as credit_limit_bucket,
        sum(credit_limit) over (partition by country order by credit_limit, id rows between unbounded preceding and current row) as cumulative_credit_limit
    from customers
)
select
    segment,
    approx_percentile(credit_limit_bucket, 0.95) as credit_limit_bucket_p95,
    count_if(segment = 'SMB') as smb_events,
    count(*) filter (where segment = 'Consumer') as consumer_rows,
    size(collect_set(segment)) as num_segments
from enriched
group by rollup (segment);
