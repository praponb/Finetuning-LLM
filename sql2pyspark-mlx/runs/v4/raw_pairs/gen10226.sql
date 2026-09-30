SELECT * FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY channel ORDER BY discount_pct ASC, id ASC) AS row_num FROM promotions) ranked WHERE row_num = 1;
