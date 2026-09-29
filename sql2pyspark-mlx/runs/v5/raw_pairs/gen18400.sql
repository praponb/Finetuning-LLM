with with_windows as (
    -- Window metrics over several different partitions
    select
        id,
        store_id,
        sale_date,
        revenue,
        lead(units) over (partition by product_id order by sale_date, id) as next_units,
        lead(revenue) over (partition by product_id order by sale_date, id) as next_revenue,
        lag(revenue) over (partition by product_id order by sale_date, id) as prev_revenue,
        row_number() over (partition by product_id order by sale_date, id) as event_seq,
        sum(units) over (order by revenue, id rows between unbounded preceding and current row) as running_units
    from store_sales
),
enriched as (
    -- Attach stores attributes
    select
        e.*,
        p.region,
        p.store_name,
        p.sqft
    from with_windows e
    left join stores p
        on e.store_id = p.id
    where p.opened_date between '2025-01-14' and '2025-08-01'
),
joined as (
    -- Attach stores attributes
    select
        s.id,
        s.store_id,
        s.revenue,
        s.next_revenue,
        s.prev_revenue,
        s.event_seq,
        s.region,
        b.city
    from enriched s
    left join stores b
        on s.store_id = b.id
),
enriched_store_sales as (
    -- Attach stores attributes
    select e.*, b.store_name
    from joined e
    inner join stores b
        on e.store_id = b.id
),
latest as (
    -- Deduplicate with ROW_NUMBER
    select *
    from (
        select *, rank() over (partition by city order by next_revenue desc) as row_num
        from enriched_store_sales
    ) x
    where row_num = 1
),
enriched_store_sales_2 as (
    -- Window metrics over several different partitions
    select
        *,
        first_value(event_seq) ignore nulls over (partition by region, city order by prev_revenue, id rows between unbounded preceding and current row) as first_known_event_seq,
        row_number() over (partition by region order by row_num, id) as seq_no,
        avg(prev_revenue) over (partition by city order by row_num, id rows between 2 preceding and 1 preceding) as prev_revenue_avg_2_prior
    from latest
),
metrics as (
    -- Ranking and gap features
    select
        *,
        ntile(4) over (partition by store_id order by revenue, id) as revenue_bucket,
        dense_rank() over (order by prev_revenue desc) as prev_revenue_dense_rank
    from enriched_store_sales_2
),
grouped as (
    -- Multi-level aggregation
    select
        case when seq_no >= 7 then 'HIGH' else 'LOW' end as seq_no_tier,
        case when revenue >= 3900 then 'LARGE' else 'SMALL' end as revenue_tier,
        count(case when region = 'North' then 1 end) as north_count,
        sum(case when city = 'New York' then revenue_bucket else 0 end) as new_york_revenue_bucket,
        avg(revenue) filter (where region = 'East') as east_avg_revenue,
        count(*) as num_rows,
        count(*) as num_rows_2
    from metrics
    group by case when seq_no >= 7 then 'HIGH' else 'LOW' end, case when revenue >= 3900 then 'LARGE' else 'SMALL' end
)
select
    seq_no_tier,
    revenue_tier,
    north_count,
    new_york_revenue_bucket,
    num_rows,
    num_rows_2
from grouped
order by seq_no_tier desc
