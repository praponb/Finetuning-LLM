select last_restock_month, inventory_count, quantity_range
from (
    select date_trunc('MONTH', last_restock) last_restock_month, count(*) inventory_count, max(quantity) - min(quantity) quantity_range
    from inventory
    where last_restock <= '2024-11-30'
    group by date_trunc('MONTH', last_restock)
) t
where quantity_range > 57;
