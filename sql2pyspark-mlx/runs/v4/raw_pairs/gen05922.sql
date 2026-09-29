select
    id,
    price,
    stock_qty,
    category,
    case when category = 'Electronics' then 1 else 0 end as is_electronics,
    upper(category) as category_upper
from products
where stock_qty >= 268 or supplier_id is not null;
