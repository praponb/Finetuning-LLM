SELECT a.*, pr.id AS promo_id, pr.promo_code, pr.discount_pct FROM orders a LEFT JOIN promotions pr ON a.channel <=> pr.channel AND a.order_date BETWEEN pr.starts_on AND pr.ends_on
