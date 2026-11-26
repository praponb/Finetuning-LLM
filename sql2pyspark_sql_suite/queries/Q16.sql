-- Q16: Hierarchy flattening (fixed-depth self-joins) and revenue roll-up
-- needs setup views: products, orders, order_items, fx_rates, category_tree
-- @id: Q16
WITH walk AS (
  SELECT
    n.node_id, n.parent_id, n.node_name,
    p1.node_id AS a1_id, p1.node_name AS a1_name,
    p2.node_id AS a2_id, p2.node_name AS a2_name,
    p3.node_id AS a3_id, p3.node_name AS a3_name
  FROM category_tree n
  LEFT JOIN category_tree p1 ON p1.node_id = n.parent_id
  LEFT JOIN category_tree p2 ON p2.node_id = p1.parent_id
  LEFT JOIN category_tree p3 ON p3.node_id = p2.parent_id
),
paths AS (
  SELECT
    node_id, parent_id, node_name,
    filter(array(a3_id, a2_id, a1_id, node_id), x -> x IS NOT NULL)                AS path_ids,
    filter(array(a3_name, a2_name, a1_name, node_name), x -> x IS NOT NULL)        AS path_names,
    parent_id IS NOT NULL AND a1_id IS NULL                                        AS is_orphan
  FROM walk
),
nodes AS (
  SELECT
    p.*,
    size(p.path_ids) - 1                                                           AS depth,
    NOT EXISTS (SELECT 1 FROM category_tree ch WHERE ch.parent_id = p.node_id)     AS is_leaf
  FROM paths p
),
product_node AS (
  SELECT
    pr.product_id,
    coalesce(n.node_id, 40) AS node_id            -- unmapped products go to Clearance
  FROM products pr
  LEFT JOIN nodes n
    ON  n.is_leaf
    AND n.node_name = pr.sub_category
    -- try_element_at: the orphan node's path has one element, element_at(.., -2) would throw
    AND lower(try_element_at(n.path_names, -2)) = pr.category
),
fx AS (
  SELECT order_id, rate_to_usd FROM (
    SELECT o.order_id, f.rate_to_usd,
           ROW_NUMBER() OVER (PARTITION BY o.order_id ORDER BY f.rate_date DESC) AS rn
    FROM orders o JOIN fx_rates f ON f.currency = o.currency AND f.rate_date <= to_date(o.order_ts)
  ) WHERE rn = 1
),
line_usd AS (
  SELECT oi.order_id, oi.product_id, oi.quantity,
         oi.quantity * oi.unit_price * (1 - oi.discount_pct) * fx.rate_to_usd AS net_usd
  FROM order_items oi
  JOIN orders o ON o.order_id = oi.order_id AND o.status IN ('DELIVERED', 'SHIPPED')
  JOIN fx       ON fx.order_id = oi.order_id
),
node_rev AS (
  SELECT
    anc_id                                       AS node_id,
    round(SUM(l.net_usd), 2)                     AS rev_usd,
    SUM(l.quantity)                              AS units,
    COUNT(DISTINCT l.order_id)                   AS orders,
    array_sort(collect_set(l.product_id))        AS products
  FROM line_usd l
  JOIN product_node pn ON pn.product_id = l.product_id
  JOIN nodes n         ON n.node_id     = pn.node_id
  LATERAL VIEW explode(n.path_ids) e AS anc_id
  GROUP BY anc_id
),
leaf_counts AS (
  SELECT anc_id AS node_id, COUNT(*) AS leaves_below
  FROM nodes n
  LATERAL VIEW explode(n.path_ids) e AS anc_id
  WHERE n.is_leaf AND n.node_id <> anc_id
  GROUP BY anc_id
)
SELECT
  n.node_id,
  n.parent_id,
  concat(repeat('  ', n.depth), n.node_name)                                     AS tree_label,
  concat_ws(' > ', n.path_names)                                                 AS full_path,
  n.depth,
  n.is_leaf,
  n.is_orphan,
  element_at(n.path_names, 1)                                                    AS root_name,
  coalesce(lc.leaves_below, 0)                                                   AS leaves_below,
  coalesce(r.rev_usd, 0)                                                         AS rev_usd,
  coalesce(r.units, 0)                                                           AS units,
  coalesce(r.orders, 0)                                                          AS orders,
  coalesce(r.products, array())                                                  AS products,
  round(100 * coalesce(r.rev_usd, 0) / nullif(pr.rev_usd, 0), 1)                 AS pct_of_parent,
  round(100 * coalesce(r.rev_usd, 0)
        / nullif(MAX(CASE WHEN n.depth = 0 AND NOT n.is_orphan THEN r.rev_usd END) OVER (), 0), 1) AS pct_of_total,
  DENSE_RANK() OVER (PARTITION BY n.parent_id ORDER BY coalesce(r.rev_usd, 0) DESC) AS sibling_rank,
  COUNT(*) OVER (PARTITION BY n.parent_id)                                       AS siblings
FROM nodes n
LEFT JOIN node_rev    r  ON r.node_id  = n.node_id
LEFT JOIN node_rev    pr ON pr.node_id = n.parent_id
LEFT JOIN leaf_counts lc ON lc.node_id = n.node_id
ORDER BY full_path;
