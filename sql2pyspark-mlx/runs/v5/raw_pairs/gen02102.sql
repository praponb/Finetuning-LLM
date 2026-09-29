with valid as (
    -- Keep recent, valid rows
    select
        *,
        floor(cost) as cost_whole,
        floor(cost) as cost_whole_2
    from products
    where brand in ('Acme', 'Globex')
),
scored as (
    -- Window metrics over several different partitions
    select
        id,
        product_name,
        category,
        price,
        cost,
        launch_date,
        tags,
        supplier_id,
        cost_whole,
        avg(cost) over (partition by category order by cost_whole asc, id asc rows between 2 preceding and current row) as cost_avg_2,
        sum(cost_whole) over w_grp as running_cost_whole
    from valid
    window w_grp as (partition by supplier_id order by price asc, id asc rows between unbounded preceding and current row)
),
enriched as (
    -- Attach suppliers attributes
    select s.*, p2.country
    from scored s
    left join suppliers p2
        on s.supplier_id = p2.id
    where p2.rating is not null
),
remaining_keys as (
    -- Set difference on keys
    select category, supplier_id
    from enriched
    except distinct
    select category, supplier_id
    from products
    where supplier_id is not null
),
reconciled as (
    -- Keep rows whose keys survive the set operation
    select s.*
    from enriched s
    inner join remaining_keys k on s.category = k.category and s.supplier_id = k.supplier_id
),
not_flagged as (
    -- Semi-join via a correlated subquery
    select s.*
    from reconciled s
    where exists (
            select 1
            from store_sales x
            where x.product_id = s.id
              and month(x.sale_date) in (5, 9, 12)
        )
)
select
    s.id,
    s.product_name,
    s.category,
    s.price,
    s.tags,
    s.supplier_id,
    s.cost_whole,
    s.cost_avg_2,
    s.running_cost_whole,
    case when product_name rlike '^(Running Shoes|Desk Lamp)$' then 'GROUP_A' when product_name rlike '^(Laptop Pro 14|wireless mouse)$' then 'PRIORITY' else 'EXTENDED' end as product_name_zone,
    (select min(x.price) from products x where x.supplier_id = s.supplier_id) as min_product_price
from not_flagged s
order by cost_avg_2 desc;
