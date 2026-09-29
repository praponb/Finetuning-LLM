select 'store' as source, opened_date from stores where region in ('East', 'North', 'Central') union all select 'inventory' as source, last_restock from inventory where last_restock < '2025-10-01'
