with deduped as (
    -- Keep one row per key (latest first)
    select *
    from (
        select *, rank() over (partition by product_id order by sale_date nulls first) as dedup_rank
        from store_sales
    ) t
    where dedup_rank = 1
)
select
    id,
    store_id,
    product_id,
    units,
    dedup_rank
from deduped;
