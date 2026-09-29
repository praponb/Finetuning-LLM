WITH numbered AS (SELECT *, ROW_NUMBER() OVER (PARTITION BY store_id ORDER BY amount DESC, id) AS pos FROM orders) SELECT id, store_id, amount, channel FROM numbered WHERE pos <= 5 ORDER BY id DESC
