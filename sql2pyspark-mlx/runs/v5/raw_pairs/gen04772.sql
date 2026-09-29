with enriched as (
    -- Attach stores attributes
    select
        e.*,
        p.sqft,
        p.city
    from store_sales e
    inner join stores p
        on e.store_id = p.id
),
cleaned as (
    -- Filter the raw rows and derive helper columns
    select *, date_format(sale_date, 'MMM yyyy') as sale_date_label
    from enriched
    where sale_date > '2025-05-01'
      and city is not null
),
enriched_store_sales as (
    -- Ranking and gap features
    select
        id,
        product_id,
        sale_date,
        units,
        sqft,
        sale_date_label,
        lead(revenue) over (partition by store_id order by revenue, id) as next_revenue,
        lag(revenue) over (partition by store_id order by revenue, id) as prev_revenue,
        lag(sqft) over (partition by store_id order by revenue, id) as prev_sqft,
        row_number() over (partition by store_id order by revenue, id) as seq_no
    from cleaned
),
ranked as (
    -- QUALIFY-style window filter
    select
        id,
        units,
        sale_date_label,
        seq_no
    from (
        select *, row_number() over (partition by product_id order by prev_sqft, id) as latest_rank
        from enriched_store_sales
    ) x
    where latest_rank = 1
),
matched_keys as (
    -- Set intersection on keys
    select id
    from ranked
    intersect
    select id
    from store_sales
    where units != 29
),
verified as (
    -- Keep rows whose keys survive the set operation
    select s.*
    from ranked s
    inner join matched_keys k on s.id = k.id
)
-- Multi-level aggregation
select
    case when units >= 42 then 'LARGE' else 'SMALL' end as units_tier,
    avg(units) as avg_units,
    round(avg(seq_no), 2) as avg_seq_no
from verified
group by case when units >= 42 then 'LARGE' else 'SMALL' end
order by avg_seq_no;
