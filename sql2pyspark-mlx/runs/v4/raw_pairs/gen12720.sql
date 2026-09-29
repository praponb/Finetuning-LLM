SELECT id, revenue, PERCENT_RANK() OVER (ORDER BY revenue DESC) AS revenue_pct_rank FROM store_sales ORDER BY 2;
