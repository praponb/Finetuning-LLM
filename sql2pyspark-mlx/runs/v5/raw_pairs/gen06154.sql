with Ranked as (
    select
        id,
        supplier_id,
        stock_qty,
        category,
        row_number() over (partition by supplier_id order by stock_qty desc, id) as row_num
    from products
)
select
    id,
    supplier_id,
    stock_qty,
    category
from Ranked
where row_num <= 3
order by id desc;
