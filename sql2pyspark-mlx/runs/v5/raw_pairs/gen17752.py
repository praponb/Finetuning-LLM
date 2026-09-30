from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.last_restock").desc(), F.col("c.id").desc())

with_latest = (
    df_stores
    .alias("s")
    .join(df_inventory.alias("c"), (F.col("c.store_id") == F.col("s.id")) & (F.col("c.last_restock") <= F.col("s.opened_date")), "inner")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.*",
        F.col("c.last_restock").alias("inventory_last_restock"),
        F.col("c.quantity"),
    )
)

result = (
    with_latest
    .cube(
        F.when(F.col("quantity") >= 202, "LARGE").otherwise("SMALL").alias("quantity_tier"),
        "city",
    )
    .agg(
        F.avg(F.when(F.col("region") == "West", F.col("sqft"))).alias("west_avg_sqft"),
        F.sum(F.when(F.col("region") == "East", F.col("quantity")).otherwise(0)).alias("east_quantity"),
        F.round(F.avg("quantity"), 2).alias("avg_quantity"),
        F.percentile_approx("sqft", [0.25, 0.5, 0.75]).alias("sqft_quartiles"),
        F.round(F.avg("sqft"), 2).alias("avg_sqft"),
        F.sum(F.when(F.col("region") == "East", F.col("sqft")).otherwise(0)).alias("east_sqft"),
    )
)
