with filtered as (
    -- Basic cleansing and defaults
    select
        *,
        date_format(last_restock, 'yyyy-MM-dd') as last_restock_label,
        add_months(last_restock, 1) as last_restock_plus_months
    from inventory
    where last_restock > date '2023-02-04'
      and store_id in (2, 3, 4)
),
joined as (
    -- Attach products attributes
    select
        src.id,
        src.store_id,
        src.product_id,
        src.last_restock,
        src.last_restock_label,
        src.last_restock_plus_months,
        d.category,
        d.product_name,
        d.price
    from filtered src
    left join products d
        on src.product_id = d.id
),
remaining_keys as (
    -- Set difference on keys
    select id
    from joined
    except
    select id
    from inventory
    where store_id in (1, 2, 3)
),
verified as (
    -- Keep rows whose keys survive the set operation
    select s.*
    from joined s
    inner join remaining_keys k on s.id = k.id
),
with_windows as (
    -- Window metrics over several different partitions
    select
        *,
        dense_rank() over (order by price nulls first) as price_dense_rank,
        count(price) over (partition by store_id, product_id) as store_id_count_price,
        dense_rank() over (order by price desc nulls first) as price_dense_rank_2,
        percent_rank() over (order by price) as price_percent_rank,
        count(price) over (partition by product_id, category) as product_id_count_price
    from verified
    where store_id in (2, 3, 5)
),
metrics as (
    -- Window metrics over several different partitions
    select
        store_id,
        product_id,
        last_restock,
        last_restock_label,
        category,
        product_name,
        price,
        store_id_count_price,
        price_dense_rank_2,
        price_percent_rank,
        product_id_count_price,
        max(price_dense_rank) over (partition by category, product_id) as category_max_price_dense_rank,
        rank() over (order by price_percent_rank nulls first) as price_percent_rank_rank,
        dense_rank() over w_ordered as price_percent_rank_dense_rank
    from with_windows
    window w_ordered as (partition by category, product_id order by price_percent_rank desc)
)
select
    s.store_id,
    s.product_id,
    s.last_restock_label,
    s.category,
    s.product_name,
    s.price,
    s.store_id_count_price,
    s.product_id_count_price,
    s.price_percent_rank_rank,
    case when product_name rlike '^(Yoga Mat|Office Chair)$' then 'CORE' when product_name rlike '^(Laptop Pro 14|wireless mouse)$' then 'GROUP_A' else 'OTHER_ZONE' end as product_name_zone,
    (select sum(o2.amount) from orders o2 where o2.store_id = s.store_id) as sum_order_amount
from metrics s
order by product_id desc;
