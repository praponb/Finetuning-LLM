SELECT * FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY store_id, product_id ORDER BY last_restock DESC NULLS LAST, id) AS latest_rank FROM inventory) ranked WHERE latest_rank = 1
