from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_grp = Window.partitionBy("product_id").orderBy("unit_price", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_part = Window.orderBy("quantity", "id").rowsBetween(-5, -1)
w3 = Window.partitionBy("order_id").orderBy(F.col("unit_price_avg_5_prior").desc_nulls_first(), "id")
w4 = Window.partitionBy("order_id").orderBy(F.col("latest_rank").desc(), "id")
w5 = Window.partitionBy("product_id").orderBy("amount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
quantity_by_product_id = df_order_items.groupBy("product_id").agg(F.sum("quantity").alias("sum_order_item_quantity"))

metrics = (
    df_order_items
    .select(
        "*",
        F.sum("quantity").over(w_grp).alias("running_quantity"),
        F.avg("unit_price").over(w_part).alias("unit_price_avg_5_prior"),
    )
)

deduped = (
    metrics
    .withColumn("latest_rank", F.row_number().over(w3))
    .filter(F.col("latest_rank") == 1)
)

joined = (
    deduped
    .alias("src")
    .join(df_orders.alias("p"), F.col("src.order_id") == F.col("p.id"), "left")
    .select("src.*", F.col("p.order_date"), F.col("p.amount"))
)

ranked = (
    joined
    .withColumn("rn", F.row_number().over(w4))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

enriched = (
    ranked
    .withColumn("first_known_unit_price_avg_5_prior", F.first("unit_price_avg_5_prior", ignorenulls=True).over(w_grp))
    .withColumn("cumulative_amount", F.sum("amount").over(w5))
)

summary = (
    enriched
    .groupBy(
        "product_id",
        F.when(F.col("cumulative_amount") >= 6870, "LARGE").otherwise("SMALL").alias("cumulative_amount_tier"),
    )
    .agg(
        F.avg("quantity").alias("avg_quantity"),
        F.bit_and("quantity").alias("quantity_bit_and"),
        F.min("quantity").alias("min_quantity"),
    )
)

result = (
    summary
    .join(quantity_by_product_id, on="product_id", how="left")
    .select(
        "product_id",
        "cumulative_amount_tier",
        "avg_quantity",
        "quantity_bit_and",
        "min_quantity",
        F.col("sum_order_item_quantity"),
    )
    .orderBy("avg_quantity")
)
