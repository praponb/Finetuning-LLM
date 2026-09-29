SELECT
    id,
    customer_id,
    shipping_fee,
    amount,
    store_id,
    DENSE_RANK() OVER (PARTITION BY customer_id ORDER BY shipping_fee DESC) AS dense_rnk,
    LAST_VALUE(store_id) OVER (ORDER BY amount DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_store_id
FROM orders;
