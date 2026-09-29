with filtered_products as (
    select *
    from products
    where brand = 'Stark' and stock_qty between 250 and 345
),
products_stats as (
    select case when brand in ('Umbrella', 'Initech') then 'group_a' else 'other' end brand_group, concat_ws(', ', sort_array(collect_set(category))) category_values
    from filtered_products
    group by case when brand in ('Umbrella', 'Initech') then 'group_a' else 'other' end
)
select *
from products_stats
order by brand_group, category_values desc;
