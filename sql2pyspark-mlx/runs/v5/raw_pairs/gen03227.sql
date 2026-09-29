select opened_date, 'account' origin from accounts union all select last_restock, 'inventory' origin from inventory where quantity between 199 and 220;
