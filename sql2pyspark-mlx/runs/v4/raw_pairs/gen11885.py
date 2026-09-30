from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.last_restock").desc(), F.col("c.id").desc())

with_latest = (
    df_stores
    .alias("s")
    .join(df_inventory.alias("c"), (F.col("c.store_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.*",
        F.col("c.quantity").alias("inventory_quantity"),
        F.col("c.last_restock").alias("inventory_last_restock"),
    )
)

result = (
    with_latest
    .withColumn("week_start", F.date_trunc("WEEK", "inventory_last_restock"))
    .withColumn("activity_month", F.date_trunc("MONTH", "inventory_last_restock"))
    .cube("week_start", "activity_month")
    .agg(
        F.count(F.when(F.col("city") == "Tokyo", 1)).alias("tokyo_rows"),
        F.sort_array(F.collect_set("region")).alias("region_list"),
        F.percentile_approx("sqft", 0.9).alias("sqft_p90"),
        F.grouping_id().alias("grouping_level"),
    )
)
