-- Q21b: Generators over maps and structs: explode(map), posexplode_outer, inline_outer
-- needs setup views: customers
-- @id: Q21b
SELECT
  c.customer_id,
  pref.pref_key,
  pref.pref_val,
  intr.interest_pos,
  intr.interest,
  hist.kind,
  hist.val
FROM customers c
LATERAL VIEW OUTER explode(c.preferences)                       pref AS pref_key, pref_val
LATERAL VIEW OUTER posexplode(c.interests)                      intr AS interest_pos, interest
LATERAL VIEW OUTER inline(
  CASE WHEN c.tier IS NULL THEN array()
       ELSE array(named_struct('kind', 'tier', 'val', c.tier),
                  named_struct('kind', 'city', 'val', c.city)) END
)                                                               hist AS kind, val
WHERE c.customer_id IN (1, 4, 5, 10)
ORDER BY c.customer_id, pref.pref_key, intr.interest_pos, hist.kind;
