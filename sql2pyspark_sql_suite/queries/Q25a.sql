-- Q25a: WITH RECURSIVE: hierarchy of any depth (compare with the fixed-depth Q16)
-- needs setup views: category_tree
-- requires Spark 4.x
-- @id: Q25a
WITH RECURSIVE tree (node_id, node_name, root_id, depth, path, id_path) AS (
  SELECT node_id, node_name, node_id, 0, node_name, array(node_id)
  FROM category_tree
  WHERE parent_id IS NULL
  UNION ALL
  SELECT c.node_id, c.node_name, t.root_id, t.depth + 1,
         concat(t.path, ' > ', c.node_name), array_append(t.id_path, c.node_id)
  FROM category_tree c
  JOIN tree t ON c.parent_id = t.node_id
  WHERE t.depth < 10                                   -- cycle guard
)
SELECT
  t.node_id,
  t.path,
  t.depth,
  t.id_path,
  size(t.id_path) - 1 = t.depth                       AS depth_consistent,
  (SELECT COUNT(*) FROM tree d WHERE array_contains(d.id_path, t.node_id) AND d.node_id <> t.node_id) AS descendants
FROM tree t
ORDER BY t.path;
