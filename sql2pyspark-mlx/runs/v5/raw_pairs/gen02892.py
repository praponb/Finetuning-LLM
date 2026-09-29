from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id").orderBy(F.col("quantity").desc_nulls_first())
w2 = Window.partitionBy("product_id").orderBy("quantity", "id").rowsBetween(-6, -1)
w3 = Window.partitionBy("store_id").orderBy(F.col("quantity").desc_nulls_last())
w4 = Window.partitionBy("product_id").orderBy(F.col("quantity_percent_rank").desc_nulls_last(), "id")

filtered = (
    df_inventory
    .filter(F.col("product_id").isin([2, 6, 7, 10]))
    .select(
        "id",
        "store_id",
        "product_id",
        "quantity",
        "last_restock",
        F.when(F.col("quantity") >= 237, "High").when(F.col("quantity") >= 111, "Medium").otherwise("Low").alias("quantity_band"),
    )
)

with_windows = (
    filtered
    .filter(F.col("last_restock") < "2024-04-23")
    .select(
        "id",
        "store_id",
        "product_id",
        "last_restock",
        F.percent_rank().over(w).alias("quantity_percent_rank"),
        F.avg("quantity").over(w2).alias("quantity_avg_6_prior"),
        F.cume_dist().over(w3).alias("quantity_cume_dist"),
    )
)

valid = (
    with_windows
    .filter(F.col("quantity_cume_dist").between(0.64, 0.66))
    .select(
        "*",
        F.round((F.col("quantity_avg_6_prior") * 100), 2).alias("adjusted_quantity_avg_6_prior"),
        F.col("quantity_cume_dist").cast("int").alias("quantity_cume_dist_int"),
    )
)

deduped = (
    valid
    .withColumn("rn", F.row_number().over(w4))
    .filter(F.col("rn") == 1)
)

result = (
    deduped
    .alias("e")
    .join(df_stores.alias("d"), F.col("e.store_id") == F.col("d.id"), "inner")
    .groupBy(F.col("d.region").alias("region"))
    .agg(
        F.count("*").alias("num_rows"),
        F.max(F.col("e.quantity_percent_rank")).alias("max_quantity_percent_rank"),
        F.sum(F.col("e.adjusted_quantity_avg_6_prior")).alias("total_adjusted_quantity_avg_6_prior"),
    )
)
