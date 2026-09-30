select month(last_restock) as last_restock_month, min(quantity) as min_quantity from inventory group by 1 order by last_restock_month, min_quantity desc
