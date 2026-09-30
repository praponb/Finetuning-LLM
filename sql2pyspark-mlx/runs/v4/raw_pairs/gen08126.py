from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy(F.col("quantity_filled").asc_nulls_last(), "id")
w2 = Window.partitionBy("store_id", "product_id").orderBy(F.col("quantity_filled").asc_nulls_last(), "id")
w3 = Window.partitionBy("store_id", "product_id").orderBy("quantity", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("product_id").orderBy(F.col("quantity_filled").desc_nulls_first())
w5 = Window.orderBy("rank_in_group", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_grp = Window.orderBy("last_restock", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w7 = Window.partitionBy("store_id", "product_id").orderBy(F.col("quantity").asc_nulls_first(), "id")

base = (
    df_inventory
    .filter(F.col("last_restock") >= F.lit("2023-09-10").cast("date"))
    .select("*", F.coalesce("quantity", F.lit(0)).alias("quantity_filled"))
)

filtered = (
    base
    .filter(F.col("last_restock") >= F.lit("2024-05-03").cast("date"))
    .select(
        "*",
        F.when(F.col("quantity_filled") >= 60, "High").when(F.col("quantity_filled") >= 14, "Medium").otherwise("Low").alias("quantity_filled_tier"),
    )
)

deduped = (
    filtered
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
    .drop("latest_rank")
)

ranked = (
    deduped
    .withColumn("rank_in_group", F.row_number().over(w2))
    .filter(F.col("rank_in_group") == 1)
)

filtered_inventory = (
    ranked
    .filter(F.col("quantity_filled") > 70)
    .select(
        "*",
        F.least("quantity", "quantity_filled").alias("least_quantity_quantity_filled"),
    )
)

scored = (
    filtered_inventory
    .withColumn("cumulative_least_quantity_quantity_filled", F.sum("least_quantity_quantity_filled").over(w3))
    .withColumn("quantity_filled_dense_rank", F.dense_rank().over(w4))
    .withColumn("last_known_quantity_filled", F.last("quantity_filled", ignorenulls=True).over(w5))
    .withColumn("running_least_quantity_quantity_filled", F.sum("least_quantity_quantity_filled").over(w_grp))
)

deduped_inventory = (
    scored
    .withColumn("rank_in_group_2", F.row_number().over(w7))
    .filter(F.col("rank_in_group_2") == 1)
)

result = (
    deduped_inventory
    .withColumn("quantity_filled_dense_rank_tier", F.when(F.col("quantity_filled_dense_rank") >= 7, "PREMIUM").otherwise("STANDARD"))
    .groupingSets(
        [["product_id", "quantity_filled_dense_rank_tier"], ["product_id"], []],
        "product_id",
        "quantity_filled_dense_rank_tier",
    )
    .agg(
        F.round(F.avg("last_known_quantity_filled"), 2).alias("avg_last_known_quantity_filled"),
        F.max("running_least_quantity_quantity_filled").alias("max_running_least_quantity_quantity_filled"),
    )
    .filter(F.col("avg_last_known_quantity_filled") > 0)
    .orderBy("max_running_least_quantity_quantity_filled")
)
