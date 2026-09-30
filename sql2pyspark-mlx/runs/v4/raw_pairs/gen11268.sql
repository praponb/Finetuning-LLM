select id, quantity, round(avg(quantity) over (order by quantity, id rows between 3 preceding and current row), 2) as quantity_moving_avg_4 from order_items order by 2;
