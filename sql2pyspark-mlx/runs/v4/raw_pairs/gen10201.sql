SELECT id, rating, PERCENT_RANK() OVER (ORDER BY rating DESC) AS rating_pct_rank FROM suppliers WHERE country = 'UK'
