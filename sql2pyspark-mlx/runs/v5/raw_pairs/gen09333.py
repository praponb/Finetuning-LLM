from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy("last_restock_plus_88d", "id").rowsBetween(-3, Window.currentRow)
w2 = Window.partitionBy("product_id", "store_id").orderBy(F.col("last4_avg_quantity").asc_nulls_first())
w3 = Window.partitionBy("product_id").orderBy("last4_avg_quantity", "id")
w4 = Window.partitionBy("store_id").orderBy("last_restock_plus_88d", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

base = (
    df_inventory
    .filter(F.col("store_id").isin([1, 2, 4, 5]))
    .withColumn("last_restock_period", F.date_trunc("WEEK", "last_restock"))
    .withColumn("last_restock_plus_88d", F.date_add("last_restock", 88))
)

enriched = (
    base
    .select(
        "id",
        "store_id",
        "product_id",
        "quantity",
        "last_restock",
        "last_restock_plus_88d",
        F.avg("quantity").over(w).alias("last4_avg_quantity"),
        F.min("quantity").over(w).alias("last4_min_quantity"),
        F.max("quantity").over(w).alias("last4_max_quantity"),
        F.sum("quantity").over(w).alias("last4_sum_quantity"),
    )
)

base_inventory = (
    enriched
    .filter(F.col("last_restock") > F.lit("2023-02-18").cast("date"))
    .select(
        "*",
        F.datediff(F.current_date(), "last_restock").alias("days_since_last_restock"),
        F.when(F.col("quantity") >= 259, "High").when(F.col("quantity") >= 90, "Medium").otherwise("Low").alias("quantity_band"),
    )
)

metrics = (
    base_inventory
    .select(
        "*",
        F.percent_rank().over(w2).alias("last4_avg_quantity_percent_rank"),
        F.row_number().over(w3).alias("row_seq"),
        F.sum("last4_max_quantity").over(w4).alias("running_last4_max_quantity"),
    )
)

enriched_inventory = (
    metrics
    .alias("e")
    .join(df_products.alias("d"), F.col("e.product_id") == F.col("d.id"), "left")
    .select("e.*", F.col("d.product_name"))
)

filtered = (
    enriched_inventory
    .filter(F.col("product_id").isin([3, 4, 8, 10]))
    .select(
        "id",
        "store_id",
        "product_id",
        "quantity",
        "last_restock_plus_88d",
        "last4_avg_quantity",
        "last4_min_quantity",
        "last4_max_quantity",
        "last4_sum_quantity",
        "row_seq",
        "running_last4_max_quantity",
        "product_name",
        F.col("quantity").cast("double").alias("quantity_str"),
    )
)

result = (
    filtered
    .groupBy(
        F.when(F.col("quantity") >= 250, "PREMIUM").otherwise("STANDARD").alias("quantity_tier"),
    )
    .agg(
        F.bit_or("quantity").alias("quantity_bit_or"),
        F.round(F.avg("last4_sum_quantity"), 2).alias("avg_last4_sum_quantity"),
    )
    .filter(F.col("quantity_bit_or") < 1000)
    .orderBy(F.col("quantity_tier").desc_nulls_last())
)
