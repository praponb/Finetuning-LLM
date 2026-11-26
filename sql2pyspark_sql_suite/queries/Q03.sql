-- Q03: PIVOT: monthly net revenue and units by category
-- needs setup views: products, orders, order_items
-- @id: Q03
SELECT *
FROM (
  SELECT
    p.category,
    date_format(o.order_ts, 'yyyy-MM')                              AS order_month,
    oi.quantity * oi.unit_price * (1 - oi.discount_pct)             AS net_local,
    oi.quantity                                                     AS units
  FROM order_items oi
  JOIN orders   o ON o.order_id  = oi.order_id
  JOIN products p ON p.product_id = oi.product_id
  WHERE o.status IN ('DELIVERED', 'SHIPPED')
) src
PIVOT (
  round(SUM(net_local), 2) AS rev,
  SUM(units)               AS units
  FOR order_month IN (
    '2024-01' AS jan, '2024-02' AS feb, '2024-03' AS mar, '2024-04' AS apr,
    '2024-05' AS may, '2024-06' AS jun, '2024-07' AS jul
  )
)
ORDER BY category;
