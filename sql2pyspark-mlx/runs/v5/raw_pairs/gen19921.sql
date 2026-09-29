SELECT * FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY status ORDER BY opened_date DESC, id) AS rank_in_group FROM accounts) t WHERE rank_in_group = 1
