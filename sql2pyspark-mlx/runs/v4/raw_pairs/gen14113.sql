SELECT *, SUBSTRING(store_name, 1, 2) AS store_name_prefix, SUBSTRING(store_name, 1, 4) AS store_name_prefix_2
FROM stores
WHERE opened_date BETWEEN '2023-10-01' AND '2024-10-01' AND MONTH(opened_date) IN (1, 6, 10);
