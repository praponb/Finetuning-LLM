from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy(F.col("quantity").desc(), "id")

filtered = (
    df_inventory
    .filter(F.col("store_id").isin([3, 5]))
    .select(
        "id",
        "store_id",
        "product_id",
        "quantity",
        F.when(F.col("quantity") >= 188, "High").when(F.col("quantity") >= 77, "Medium").otherwise("Low").alias("quantity_tier"),
    )
)

latest = (
    filtered
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
)

valid = (
    latest
    .filter(F.col("store_id").isin([1, 4, 5]))
    .select(
        "*",
        F.round((F.col("quantity") / F.col("rank_in_group")), 2).alias("quantity_per_rank_in_group"),
    )
)

result = (
    valid
    .groupBy(
        F.when(F.col("quantity_per_rank_in_group") >= 81, "HIGH").otherwise("LOW").alias("quantity_per_rank_in_group_tier"),
    )
    .agg(
        F.stddev_samp("quantity").alias("quantity_stddev"),
        F.round(F.avg("quantity_per_rank_in_group"), 2).alias("avg_quantity_per_rank_in_group"),
        F.max("quantity_per_rank_in_group").alias("max_quantity_per_rank_in_group"),
        F.avg("quantity").alias("avg_quantity"),
    )
)
