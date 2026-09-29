with filtered as (
    -- Filter the raw rows and derive helper columns
    select *, date_format(sale_date, 'yyyy-MM-dd') as sale_date_label
    from store_sales
    where sale_date > to_date('2023-05-03')
),
with_windows as (
    -- Running and moving aggregates
    select
        id,
        store_id,
        product_id,
        sale_date,
        revenue,
        sale_date_label,
        cume_dist() over (partition by store_id order by units nulls last) as units_cume_dist,
        dense_rank() over (partition by store_id order by units desc nulls first) as units_dense_rank,
        sum(revenue) over (partition by product_id order by sale_date, id rows between unbounded preceding and current row) as running_revenue
    from filtered
),
enriched as (
    -- Attach products attributes
    select
        a.id,
        a.product_id,
        a.sale_date,
        a.running_revenue,
        p.price
    from with_windows a
    inner join products p
        on a.product_id = p.id
),
joined as (
    -- Attach products attributes
    select
        s.id,
        s.product_id,
        s.sale_date,
        s.running_revenue,
        p.brand,
        p.price as product_price
    from enriched s
    inner join products p
        on s.product_id = p.id
    where p.stock_qty between 138 and 219
),
scored as (
    -- Ranking and gap features
    select
        id,
        product_id,
        sale_date,
        brand,
        product_price,
        row_number() over w_hist as event_seq,
        cume_dist() over (partition by brand order by running_revenue desc nulls first) as running_revenue_cume_dist,
        percent_rank() over (partition by brand, product_id order by product_price desc nulls last) as product_price_percent_rank
    from joined
    window w_hist as (partition by brand order by running_revenue desc, id)
)
select
    id,
    sale_date,
    product_price,
    running_revenue_cume_dist,
    product_price_percent_rank
from scored
order by product_price desc;
