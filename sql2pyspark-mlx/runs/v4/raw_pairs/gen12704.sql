with not_flagged as (
    -- Anti-join via a correlated subquery
    select s.*
    from suppliers s
    where not exists (
            select 1
            from products c
            where c.supplier_id = s.id
              and array_contains(c.tags, 'bestseller')
        )
),
filtered as (
    -- Filter the raw rows and derive helper columns
    select
        id,
        country,
        city,
        rating,
        concat(country, ', ', city) country_city,
        coalesce(city, 'N/A') resolved_city
    from not_flagged
    where rating < 3.6
),
latest_match as (
    -- Latest matching product per row (LATERAL ... LIMIT 1)
    select
        s.*,
        top1.product_stock_qty,
        top1.product_price
    from filtered s
    left join lateral (
            select c.stock_qty as product_stock_qty, c.price as product_price
            from products c
            where c.supplier_id = s.id
            order by c.price desc, c.id desc
            limit 1
        ) top1 on true
),
metrics as (
    -- Running and moving aggregates
    select
        *,
        avg(product_stock_qty) over w_hist product_stock_qty_avg_5,
        lead(rating, 2, 0) over (partition by city order by product_price, id) next_rating
    from latest_match
    window w_hist as (partition by city, country order by rating, id rows between 5 preceding and current row)
),
metrics_suppliers as (
    -- Running and moving aggregates
    select
        id,
        country,
        city,
        rating,
        country_city,
        resolved_city,
        product_stock_qty,
        product_price,
        product_stock_qty_avg_5,
        next_rating,
        lead(product_stock_qty, 1) over (order by rating, id) next_product_stock_qty,
        ntile(3) over w_running rating_bucket,
        cume_dist() over w_part product_stock_qty_avg_5_cume_dist,
        dense_rank() over (partition by country, city order by rating nulls last) rating_dense_rank
    from metrics
    window w_running as (partition by city, country order by rating, id), w_part as (order by product_stock_qty_avg_5 nulls first)
),
deduped as (
    -- Deduplicate with ROW_NUMBER
    select *
    from (
        select *, row_number() over (partition by city order by rating_bucket nulls first, id) as dedup_rank
        from metrics_suppliers
    ) t
    where dedup_rank = 1
),
remaining_keys as (
    -- Set difference on keys
    select country, id
    from deduped
    except distinct
    select country, id
    from suppliers
    where country in ('DE', 'UK')
),
kept_rows as (
    -- Keep rows whose keys survive the set operation
    select s.*
    from deduped s
    inner join remaining_keys k on s.country = k.country and s.id = k.id
)
select
    country,
    city,
    rating,
    country_city,
    resolved_city,
    next_product_stock_qty,
    rating_bucket,
    product_stock_qty_avg_5_cume_dist,
    dedup_rank
from kept_rows
order by city desc;
