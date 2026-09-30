from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy("discount", "id").rowsBetween(-6, -1)
w2 = Window.partitionBy("product_id").orderBy("quantity", "id")
w3 = Window.partitionBy("order_id").orderBy("quantity_avg_6_prior", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("order_id").orderBy("quantity_avg_6_prior", "id").rowsBetween(-3, Window.currentRow)
w5 = Window.orderBy("quantity", "id").rowsBetween(-6, -1)
w6 = Window.partitionBy("order_id").orderBy(F.col("next_quantity").desc_nulls_first(), "id")

with_dims = (
    df_order_items
    .alias("e")
    .join(df_orders.alias("d"), F.col("e.order_id") == F.col("d.id"), "left")
    .filter(F.col("d.shipping_fee") <= 35)
    .select(
        "e.id",
        "e.order_id",
        "e.product_id",
        "e.quantity",
        F.col("d.channel"),
        F.col("d.discount"),
        F.col("d.status"),
    )
)

base = (
    with_dims
    .filter(F.col("product_id").isNotNull() & ~F.col("status").isin(["CANCELLED", "COMPLETED", "PENDING"]))
    .withColumn("discount_tier", F.when(F.col("discount") >= 0.39, "High").when(F.col("discount") >= 0.16, "Medium").otherwise("Low"))
)

scored = (
    base
    .select(
        "id",
        "order_id",
        "quantity",
        "discount_tier",
        F.avg("quantity").over(w).alias("quantity_avg_6_prior"),
        F.lead("quantity", 1, 0).over(w2).alias("next_quantity"),
    )
)

enriched = (
    scored
    .withColumn("cumulative_quantity_avg_6_prior", F.sum("quantity_avg_6_prior").over(w3))
    .withColumn("next_quantity_avg_3", F.avg("next_quantity").over(w4))
    .withColumn("quantity_avg_6_prior_avg_6_prior", F.avg("quantity_avg_6_prior").over(w5))
)

ranked = (
    enriched
    .withColumn("latest_rank", F.row_number().over(w6))
    .filter(F.col("latest_rank") == 1)
    .select(
        "id",
        "order_id",
        "cumulative_quantity_avg_6_prior",
        "next_quantity_avg_3",
        "quantity_avg_6_prior_avg_6_prior",
    )
)

result = (
    ranked
    .groupBy("order_id")
    .agg(
        F.max("cumulative_quantity_avg_6_prior").alias("max_cumulative_quantity_avg_6_prior"),
        F.round(F.avg("cumulative_quantity_avg_6_prior"), 2).alias("avg_cumulative_quantity_avg_6_prior"),
        F.max("cumulative_quantity_avg_6_prior").alias("max_cumulative_quantity_avg_6_prior_2"),
        F.percentile_approx("next_quantity_avg_3", [0.25, 0.5, 0.75]).alias("next_quantity_avg_3_quartiles"),
        F.count("*").alias("cnt"),
    )
)
