with cleaned as (
    -- Basic cleansing and defaults
    select
        id,
        store_id,
        product_id,
        sale_date,
        units,
        revenue,
        case when revenue >= 4400 then 'High' when revenue >= 700 then 'Medium' else 'Low' end as revenue_band,
        datediff(current_date(), sale_date) as days_since_sale_date
    from store_sales
    where store_id in (1, 2, 4, 6)
),
with_windows as (
    -- Ranking and gap features
    select
        id,
        store_id,
        product_id,
        units,
        revenue_band,
        avg(revenue) over (order by sale_date asc, id asc rows between 6 preceding and 1 preceding) as revenue_avg_6_prior,
        round(revenue / sum(revenue) over (partition by store_id), 4) as revenue_share,
        count(units) over (partition by product_id, store_id) as product_id_count_units
    from cleaned
),
enriched as (
    -- Attach stores attributes
    select
        e.id,
        e.store_id,
        e.units,
        e.revenue_avg_6_prior,
        e.revenue_share,
        e.product_id_count_units,
        p.store_name,
        p.city
    from with_windows e
    left join stores p
        on e.store_id = p.id
),
deduped as (
    -- Keep one row per key (latest first)
    select *, rank() over (partition by store_id, city order by revenue_share desc nulls last) as dedup_rank
    from enriched
    qualify dedup_rank = 1
),
joined as (
    -- Attach stores attributes
    select
        s.*,
        b.store_name as store_store_name,
        b.sqft,
        b.city as store_city
    from deduped s
    inner join stores b
        on s.store_id = b.id
),
deduped_store_sales as (
    -- Deduplicate with ROW_NUMBER
    select *, row_number() over (partition by store_city, store_id order by dedup_rank desc, id asc) as rank_in_group
    from joined
    qualify rank_in_group = 1
),
enriched_store_sales as (
    -- Ranking and gap features
    select
        id,
        units,
        product_id_count_units,
        store_name,
        city,
        store_store_name,
        sqft,
        store_city,
        rank_in_group,
        lead(revenue_share, 2) over (partition by store_city order by units asc, id asc) as next_revenue_share,
        ntile(5) over (partition by store_id order by sqft asc, id asc) as sqft_bucket,
        avg(sqft) over (partition by city order by units asc, id asc rows between 6 preceding and 1 preceding) as sqft_avg_6_prior,
        row_number() over (partition by city, store_city order by product_id_count_units asc, id asc) as seq_no
    from deduped_store_sales
)
select
    id,
    store_name,
    store_store_name,
    rank_in_group,
    sqft_avg_6_prior,
    seq_no,
    case when store_name rlike '^(Siam Flagship|Old Town)$' then 'EXTENDED' when store_name rlike '^(Downtown Express|Airport Kiosk)$' then 'GROUP_A' else 'GROUP_B' end as store_name_zone
from enriched_store_sales
order by seq_no desc nulls first, id desc;
