with numbered as (
    select
        id,
        store_id,
        sale_date,
        units,
        row_number() over (partition by store_id order by sale_date, id) as row_num
    from store_sales
)
select *
from numbered
where row_num <= 5;
