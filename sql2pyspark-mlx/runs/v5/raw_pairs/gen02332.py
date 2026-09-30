from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city")
w2 = Window.partitionBy("supplier_id")

with_dims = (
    df_products
    .alias("src")
    .join(df_suppliers.alias("s"), F.col("src.supplier_id") == F.col("s.id"), "left")
    .select(
        "src.id",
        "src.price",
        "src.cost",
        "src.stock_qty",
        "src.supplier_id",
        F.col("s.city"),
        F.col("s.rating"),
    )
)

not_flagged = (
    with_dims
    .alias("s")
    .join(df_order_items.alias("x"), (F.col("x.product_id") == F.col("s.id")) & (F.col("x.order_id").isin([10, 30])), "left_anti")
)

remaining_keys = (
    not_flagged
    .select("supplier_id", "id")
    .exceptAll(
        df_products
        .filter(F.col("supplier_id").isin([8, 15, 36]))
        .select("supplier_id", "id")
    )
)

kept_rows = (
    not_flagged
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.supplier_id") == F.col("k.supplier_id")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

scored = (
    kept_rows
    .withColumn("rating_share", F.round((F.col("rating") / F.sum("rating").over(w)), 4))
)

remaining_keys_2 = (
    scored
    .select("id", "supplier_id")
    .subtract(
        df_products
        .filter(F.col("category").isin(["Books", "Electronics"]))
        .select("id", "supplier_id")
    )
)

reconciled = (
    scored
    .alias("s")
    .join(remaining_keys_2.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.supplier_id") == F.col("k.supplier_id")), "inner")
    .select("s.*")
)

metrics = (
    reconciled
    .select(
        "*",
        F.stddev_samp("rating_share").over(w2).alias("supplier_id_stddev_samp_rating_share"),
        F.stddev_samp("rating").over(w2).alias("supplier_id_stddev_samp_rating"),
    )
)

remaining_keys_3 = (
    metrics
    .select("id")
    .intersectAll(
        df_products
        .filter(F.col("launch_date").between("2023-06-01", "2023-08-26"))
        .select("id")
    )
)

kept_rows_products = (
    metrics
    .alias("s")
    .join(remaining_keys_3.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

result = (
    kept_rows_products
    .groupBy("city")
    .agg(
        F.round(F.avg("supplier_id_stddev_samp_rating"), 2).alias("avg_supplier_id_stddev_samp_rating"),
        F.round(F.sum("stock_qty") / F.nullif(F.count(F.when(F.col("city") == "New York", 1)), F.lit(0)), 2).alias("stock_qty_per_new_york"),
        F.min("price").alias("min_price"),
    )
)
