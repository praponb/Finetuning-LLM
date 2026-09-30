with deduped as (
    select *
    from (
        select *, row_number() over (partition by order_id order by quantity desc, id) as row_num
        from order_items
    ) x
    where row_num = 1
)
select
    case when quantity >= 7 then 'PREMIUM' else 'STANDARD' end as quantity_tier,
    order_id,
    count(distinct id) as distinct_ids,
    percentile_approx(quantity, array(0.25, 0.5, 0.75)) as quantity_quartiles,
    corr(quantity, unit_price) as corr_quantity_unit_price,
    count(distinct id) as distinct_ids_2
from deduped
group by case when quantity >= 7 then 'PREMIUM' else 'STANDARD' end, order_id
order by quantity_tier desc nulls last;
