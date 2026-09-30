from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("region", "city").orderBy("sqft", "id").rowsBetween(-6, Window.currentRow)
w2 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.quantity").desc(), F.col("c.id").desc())
w_ordered = Window.partitionBy("region").orderBy("sqft_times_quantity", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.orderBy(F.col("sqft_times_quantity").desc(), "id")

kept = (
    df_stores
    .alias("s")
    .join(df_inventory.alias("c"), (F.col("c.store_id") == F.col("s.id")) & (F.col("c.product_id").isin([1, 5, 6, 8])), "left_anti")
)

enriched = (
    kept
    .withColumn("last7_max_sqft", F.max("sqft").over(w))
    .withColumn("last7_avg_sqft", F.avg("sqft").over(w))
    .withColumn("last7_count_sqft", F.count("sqft").over(w))
)

latest_match = (
    enriched
    .alias("s")
    .join(df_inventory.alias("c"), (F.col("c.store_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w2))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.city",
        "s.region",
        "s.sqft",
        F.col("c.last_restock"),
        F.col("c.quantity"),
    )
)

filtered = (
    latest_match
    .filter(F.col("quantity") > 153)
    .select(
        "*",
        (F.col("sqft") * F.col("quantity")).alias("sqft_times_quantity"),
        F.add_months("last_restock", 1).alias("last_restock_plus_months"),
        F.coalesce("region", F.lit("UNKNOWN")).alias("region_clean"),
    )
)

metrics = (
    filtered
    .withColumn("running_avg_sqft_times_quantity", F.avg("sqft_times_quantity").over(w_ordered))
    .withColumn("running_max_sqft_times_quantity", F.max("sqft_times_quantity").over(w_ordered))
    .withColumn("running_min_sqft_times_quantity", F.min("sqft_times_quantity").over(w_ordered))
    .withColumn("running_count_sqft_times_quantity", F.count("sqft_times_quantity").over(w_ordered))
    .withColumn("sqft_times_quantity_bucket", F.ntile(5).over(w4))
)

result = (
    metrics
    .select(
        "id",
        "quantity",
        "last_restock_plus_months",
        "region_clean",
        "running_count_sqft_times_quantity",
        "sqft_times_quantity_bucket",
    )
    .orderBy("last_restock_plus_months")
)
