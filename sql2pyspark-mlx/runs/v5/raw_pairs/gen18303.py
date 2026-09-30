from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id")
w2 = Window.orderBy("quantity", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("order_id").orderBy(F.col("cumulative_unit_price").desc(), "id")
w4 = Window.partitionBy("brand").orderBy("order_id_min_quantity", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("product_id").orderBy("unit_price", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.partitionBy("order_id").orderBy("first_known_cumulative_unit_price", "id")
w7 = Window.partitionBy("order_id", "brand").orderBy("cumulative_unit_price_2", "id")
w8 = Window.partitionBy("order_id", "brand").orderBy("cumulative_unit_price_bucket", "id").rowsBetween(-5, Window.currentRow)
w9 = Window.partitionBy("brand").orderBy("quantity", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w10 = Window.partitionBy("order_id").orderBy("cumulative_unit_price", "id")
w11 = Window.partitionBy("order_id", "brand")
w12 = Window.partitionBy("brand").orderBy("cumulative_unit_price_2_bucket")

joined = (
    df_order_items
    .alias("e")
    .join(df_products.alias("b"), F.col("e.product_id").eqNullSafe(F.col("b.id")), "left")
    .select("e.*", F.col("b.brand"))
)

enriched = (
    joined
    .select(
        "*",
        F.min("quantity").over(w).alias("order_id_min_quantity"),
        F.sum("unit_price").over(w2).alias("cumulative_unit_price"),
    )
)

metrics = (
    enriched
    .filter(F.col("brand").isNotNull())
    .select(
        "id",
        "order_id",
        "quantity",
        "brand",
        "cumulative_unit_price",
        F.ntile(10).over(w3).alias("cumulative_unit_price_bucket"),
        F.first("cumulative_unit_price", ignorenulls=True).over(w4).alias("first_known_cumulative_unit_price"),
        F.sum("unit_price").over(w5).alias("cumulative_unit_price_2"),
    )
)

enriched_order_items = (
    metrics
    .withColumn("next_cumulative_unit_price_2", F.lead("cumulative_unit_price_2", 2, 0).over(w6))
    .withColumn("cumulative_unit_price_2_bucket", F.ntile(4).over(w7))
    .withColumn("quantity_avg_5", F.avg("quantity").over(w8))
    .withColumn("running_quantity", F.sum("quantity").over(w9))
    .withColumn("seq_no", F.row_number().over(w10))
)

scored = (
    enriched_order_items
    .select(
        "*",
        F.round((F.col("first_known_cumulative_unit_price") / F.sum("first_known_cumulative_unit_price").over(w11)), 4).alias("first_known_cumulative_unit_price_share"),
        F.cume_dist().over(w12).alias("cumulative_unit_price_2_bucket_cume_dist"),
    )
)

result = (
    scored
    .groupBy(
        F.when(F.col("cumulative_unit_price") >= 2243, "LARGE").otherwise("SMALL").alias("cumulative_unit_price_tier"),
        "order_id",
        F.when(F.col("quantity") >= 9, "PREMIUM").otherwise("STANDARD").alias("quantity_tier"),
    )
    .agg(
        F.count(F.when(F.col("brand") == "Stark", 1)).alias("stark_count"),
        F.avg("cumulative_unit_price_2").alias("avg_cumulative_unit_price_2"),
        F.round(F.avg("cumulative_unit_price_2_bucket"), 2).alias("avg_cumulative_unit_price_2_bucket"),
    )
)
