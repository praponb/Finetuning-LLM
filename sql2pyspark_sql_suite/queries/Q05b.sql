-- Q05b: LATERAL VIEW posexplode / explode OUTER / inline over structs
-- needs setup views: products
-- @id: Q05b
SELECT
  pa.product_id,
  pa.product_name,
  t.pos      AS tag_pos,
  t.tag,
  sz.size_label,
  pa.a.warranty.months                                      AS warranty_months,
  coalesce(pa.a.weight_g, -1)                               AS weight_g
FROM (
  SELECT product_id, product_name,
         from_json(attributes_json,
           'color STRING, weight_g INT, sizes ARRAY<STRING>, tags ARRAY<STRING>, warranty STRUCT<months: INT>') AS a
  FROM products
) pa
LATERAL VIEW OUTER posexplode(pa.a.tags)  t  AS pos, tag
LATERAL VIEW OUTER explode(pa.a.sizes)    sz AS size_label
ORDER BY pa.product_id, tag_pos, size_label;
