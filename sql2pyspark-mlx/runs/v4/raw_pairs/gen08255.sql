with exploded as (
    -- Unnest the array into one row per element
    select
        id,
        service,
        status_code,
        attr_key,
        attr_value
    from app_logs
    lateral view outer explode(context) li as attr_key, attr_value
),
metrics as (
    -- Ranking and gap features
    select
        id,
        service,
        status_code,
        attr_key,
        attr_value,
        round(status_code / sum(status_code) over (partition by service, attr_key), 4) as status_code_share,
        min(status_code) over (partition by attr_key) as attr_key_min_status_code,
        avg(status_code) over (partition by service) as service_avg_status_code,
        rank() over w_ordered as status_code_rank
    from exploded
    window w_ordered as (order by status_code asc)
),
filtered as (
    -- Filter the raw rows and derive helper columns
    select *, cast(status_code_rank as double) as status_code_rank_str
    from metrics
    where attr_key is not null
)
-- Aggregate with conditional metrics
select
    service,
    count(*) as cnt,
    count(distinct id) as distinct_ids,
    percentile_approx(attr_key_min_status_code, 0.95) as attr_key_min_status_code_p95,
    sum(case when service = 'billing' then service_avg_status_code else 0 end) as billing_service_avg_status_code,
    count(*) filter (where service = 'checkout') as checkout_rows,
    sort_array(collect_set(attr_key)) as attr_key_list
from filtered
group by service
order by checkout_rows desc;
