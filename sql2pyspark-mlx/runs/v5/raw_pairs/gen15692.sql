select id, order_date, initcap(trim(channel)) as channel_title from orders where order_date between '2024-07-01' and '2025-06-28' order by channel_title
