select product_id, datediff(current_date(), last_restock) as days_since_last_restock
from inventory;
