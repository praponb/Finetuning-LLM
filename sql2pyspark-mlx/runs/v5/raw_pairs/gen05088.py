from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_grp = Window.orderBy("unit_price", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.orderBy("quantity", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_grp_2 = Window.partitionBy("order_id").orderBy("unit_price", "id")
w4 = Window.partitionBy("order_id", "product_id").orderBy("unit_price", "id")
w5 = Window.partitionBy("order_id")
w_hist = Window.partitionBy("order_id").orderBy(F.col("seq_no_2").desc())
w7 = Window.partitionBy("product_id", "order_id")
w8 = Window.orderBy(F.col("seq_no").desc_nulls_last())

cleaned = (
    df_order_items
    .filter(F.col("product_id").isNotNull())
    .select(
        "*",
        F.when(F.col("quantity") >= 7, "High").when(F.col("quantity") >= 4, "Medium").otherwise("Low").alias("quantity_tier"),
    )
)

metrics = (
    cleaned
    .withColumn("cumulative_unit_price", F.sum("unit_price").over(w_grp))
    .withColumn("first_known_quantity", F.first("quantity", ignorenulls=True).over(w2))
    .withColumn("seq_no", F.row_number().over(w_grp_2))
    .withColumn("seq_no_2", F.row_number().over(w4))
)

kept_keys = (
    metrics
    .select("product_id", "order_id")
    .intersect(
        df_order_items
        .filter(F.col("order_id").isin([5, 28]))
        .select("product_id", "order_id")
    )
)

kept_rows = (
    metrics
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.product_id") == F.col("k.product_id")) & (F.col("s.order_id") == F.col("k.order_id")), "inner")
    .select("s.*")
)

metrics_order_items = (
    kept_rows
    .filter(F.col("order_id").isin([13, 21, 22, 36]))
    .select(
        "*",
        F.round((F.col("seq_no") / F.sum("seq_no").over(w5)), 4).alias("seq_no_share"),
        F.cume_dist().over(w_hist).alias("seq_no_2_cume_dist"),
        F.round((F.col("first_known_quantity") / F.sum("first_known_quantity").over(w7)), 4).alias("first_known_quantity_share"),
        F.percent_rank().over(w8).alias("seq_no_percent_rank"),
    )
)

result = (
    metrics_order_items
    .alias("src")
    .join(df_products.alias("p"), F.col("src.product_id") == F.col("p.id"), "left")
    .groupBy(F.col("p.brand").alias("brand"))
    .agg(
        F.count("*").alias("num_rows"),
        F.avg(F.col("src.seq_no")).alias("avg_seq_no"),
    )
    .orderBy(F.col("brand").desc())
)
