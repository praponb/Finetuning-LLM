select * from shipments where shipped_date between '2024-08-01' and '2024-09-17' and year(delivered_date) >= 2025 and order_id in (9, 19, 20, 40) order by shipped_date asc, cost desc;
