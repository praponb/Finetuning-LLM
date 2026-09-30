with base as (
    -- Basic cleansing and defaults
    select
        id,
        name,
        email,
        country,
        city,
        segment,
        is_active,
        credit_limit,
        age,
        concat(country, ' ', name) as country_name,
        concat(segment, ' ', city) as segment_city,
        coalesce(segment, 'N/A') as segment_clean
    from customers
    where signup_date > date '2023-08-02'
),
matched_keys as (
    -- Set intersection on keys
    select segment
    from base
    intersect all
    select segment
    from customers
    where segment = 'Consumer'
),
verified as (
    -- Keep rows whose keys survive the set operation
    select s.*
    from base s
    inner join matched_keys k on s.segment = k.segment
),
metrics as (
    -- Window metrics over several different partitions
    select
        id,
        name,
        email,
        country,
        city,
        segment,
        is_active,
        credit_limit,
        age,
        country_name,
        segment_city,
        segment_clean,
        corr(credit_limit, age) over (partition by segment) as corr_credit_limit_age,
        round(credit_limit / sum(credit_limit) over (partition by country, segment), 4) as credit_limit_share,
        percent_rank() over (partition by country order by age nulls first) as age_percent_rank,
        max(age) over (partition by segment) as segment_max_age,
        percent_rank() over (order by age desc nulls last) as age_percent_rank_2
    from verified
),
metrics_customers as (
    -- Window metrics over several different partitions
    select
        *,
        count(credit_limit_share) over (partition by segment) as segment_count_credit_limit_share,
        percent_rank() over w_ordered as age_percent_rank_percent_rank,
        count(age_percent_rank) over (partition by country) as country_count_age_percent_rank
    from metrics
    window w_ordered as (partition by city, country order by age_percent_rank desc nulls first)
),
metrics_customers_2 as (
    -- Ranking and gap features
    select
        *,
        percent_rank() over (partition by city order by credit_limit_share nulls first) as credit_limit_share_percent_rank,
        min(segment_count_credit_limit_share) over (partition by city, segment) as city_min_segment_count_credit_limit_share
    from metrics_customers
)
-- Multi-level aggregation
select
    segment,
    percentile_approx(country_count_age_percent_rank, array(0.25, 0.5, 0.75)) as country_count_age_percent_rank_quartiles,
    round(sum(corr_credit_limit_age) / nullif(count(case when segment = 'Corporate' then 1 end), 0), 2) as corr_credit_limit_age_per_corporate,
    sum(credit_limit_share_percent_rank) as total_credit_limit_share_percent_rank,
    approx_percentile(city_min_segment_count_credit_limit_share, 0.5) as city_min_segment_count_credit_limit_share_p50
from metrics_customers_2
group by segment;
