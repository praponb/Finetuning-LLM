from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy("unit_price", "id").rowsBetween(-6, -1)
w2 = Window.partitionBy("product_id")
w3 = Window.partitionBy("order_id", "product_id").orderBy("unit_price", "id")

with_windows = (
    df_order_items
    .withColumn("unit_price_avg_6_prior", F.avg("unit_price").over(w))
    .withColumn("unit_price_share", F.round((F.col("unit_price") / F.sum("unit_price").over(w2)), 4))
    .withColumn("seq_no", F.row_number().over(w3))
)

result = (
    with_windows
    .withColumn("quantity_tier", F.when(F.col("quantity") >= 9, "LARGE").otherwise("SMALL"))
    .withColumn("unit_price_share_tier", F.when(F.col("unit_price_share") >= 0.71, "LARGE").otherwise("SMALL"))
    .groupingSets(
        [["quantity_tier", "unit_price_share_tier", "seq_no"], ["quantity_tier", "unit_price_share_tier"]],
        "quantity_tier",
        "unit_price_share_tier",
        "seq_no",
    )
    .agg(
        F.percentile_approx("quantity", [0.25, 0.5, 0.75]).alias("quantity_quartiles"),
        F.round(F.avg("unit_price"), 2).alias("avg_unit_price"),
        F.percentile_approx("unit_price_share", 0.5).alias("unit_price_share_p50"),
        F.grouping("quantity_tier").alias("gid"),
    )
)
