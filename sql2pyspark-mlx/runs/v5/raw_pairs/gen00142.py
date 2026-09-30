from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.quantity").desc(), F.col("c.id").desc())

with_last = (
    df_stores
    .alias("s")
    .join(df_inventory.alias("c"), (F.col("c.store_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.*",
        F.col("c.quantity").alias("inventory_quantity"),
        F.col("c.last_restock"),
    )
)

result = (
    with_last
    .groupBy("city")
    .agg(
        F.size(F.collect_set("city")).alias("num_cities"),
        F.round(F.sum("inventory_quantity") / F.nullif(F.count(F.when(F.col("city") == "London", 1)), F.lit(0)), 2).alias("inventory_quantity_per_london"),
        F.count("*").alias("num_rows"),
        F.avg("inventory_quantity").alias("avg_inventory_quantity"),
        F.percentile_approx("inventory_quantity", 0.9).alias("inventory_quantity_p90"),
    )
    .filter(F.col("avg_inventory_quantity") < 1000000)
    .orderBy(F.col("num_rows").asc_nulls_first())
)
