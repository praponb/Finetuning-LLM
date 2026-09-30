with scored as (
    -- Window metrics over several different partitions
    select
        *,
        corr(credit_limit, age) over (partition by country, city) as corr_credit_limit_age,
        ntile(10) over (partition by segment order by age desc, id) as age_bucket
    from customers
),
not_flagged as (
    -- Anti-join via a correlated subquery
    select s.*
    from scored s
    where not exists (
            select 1
            from subscriptions x
            where x.customer_id = s.id
              and x.status in ('ACTIVE', 'EXPIRED')
        )
),
cleaned as (
    -- Keep recent, valid rows
    select
        id,
        name,
        country,
        city,
        segment,
        signup_date,
        is_active,
        age,
        age_bucket,
        datediff(current_date(), signup_date) as days_since_signup_date,
        concat_ws('-', segment, email) as segment_email_key,
        coalesce(segment, 'OTHER') as resolved_segment
    from not_flagged
    where city is not null
      and credit_limit > 10900
),
with_latest as (
    -- Latest matching session per row (LATERAL ... LIMIT 1)
    select
        s.*,
        latest.session_country,
        latest.session_browser
    from cleaned s
    join lateral (
            select c.country as session_country, c.browser as session_browser
            from sessions c
            where c.customer_id = s.id
            order by c.started_at desc, c.id desc
            limit 1
        ) latest on true
),
matched_keys as (
    -- Set intersection on keys
    select country, segment
    from with_latest
    intersect all
    select country, segment
    from customers
    where email is not null
),
reconciled as (
    -- Keep rows whose keys survive the set operation
    select s.*
    from with_latest s
    join matched_keys k on s.country = k.country and s.segment = k.segment
)
-- Aggregate with conditional metrics
select
    date_trunc('quarter', signup_date) as quarter_start,
    sum(days_since_signup_date) filter (where segment = 'Corporate') as corporate_total_days_since_signup_date,
    count(*) as event_count,
    sum(age) filter (where session_browser = 'Edge') as edge_total_age,
    round(avg(age_bucket), 2) as avg_age_bucket,
    approx_percentile(age_bucket, 0.95) as age_bucket_p95,
    sort_array(collect_set(session_browser)) as session_browser_list
from reconciled
group by date_trunc('quarter', signup_date)
order by edge_total_age desc nulls last;
