SELECT id, store_id, units, PERCENT_RANK() OVER (PARTITION BY store_id ORDER BY units) AS units_pct_rank FROM store_sales
