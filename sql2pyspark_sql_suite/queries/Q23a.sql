-- Q23a: NOT IN against a subquery that yields a NULL returns NO rows; NOT EXISTS does
-- needs setup views: products
-- @id: Q23a
SELECT 'not_in' AS method, p.product_id, p.sub_category
FROM products p
WHERE p.sub_category NOT IN (SELECT sub_category FROM products WHERE category IN ('misc', 'books'))
UNION ALL
SELECT 'not_exists' AS method, p.product_id, p.sub_category
FROM products p
WHERE NOT EXISTS (SELECT 1 FROM products q
                  WHERE q.category IN ('misc', 'books') AND q.sub_category = p.sub_category)
UNION ALL
SELECT 'in_with_null_list' AS method, p.product_id, p.sub_category
FROM products p
WHERE p.sub_category IN ('audio', NULL)           -- TRUE for audio, NULL (=filtered) otherwise
UNION ALL
SELECT 'not_in_with_null_list' AS method, p.product_id, p.sub_category
FROM products p
WHERE p.sub_category NOT IN ('audio', NULL)       -- never TRUE
ORDER BY method, product_id;
