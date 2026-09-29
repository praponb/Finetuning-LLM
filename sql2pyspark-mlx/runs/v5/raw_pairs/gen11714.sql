select last_restock from inventory union select end_date as last_restock from subscriptions where status in ('EXPIRED', 'PAUSED') order by last_restock;
