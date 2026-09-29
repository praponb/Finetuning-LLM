from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.quantity").desc(), F.col("c.id").desc())
w2 = Window.partitionBy("region").orderBy("opened_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("region")

filtered = (
    df_stores
    .filter((F.col("opened_date") > F.to_date(F.lit("2023-04-26"))) & (F.col("sqft") >= 10862))
    .select(
        "id",
        "store_name",
        "city",
        "region",
        "opened_date",
        F.concat("store_name", F.lit(" - "), "city").alias("store_name_city"),
        F.coalesce("region", F.lit("NONE")).alias("region_clean"),
    )
)

cleaned = (
    filtered
    .filter(F.col("region").isNotNull() & F.col("region").isin(["West", "East", "Central"]))
    .select(
        "id",
        "store_name",
        "city",
        "region",
        "opened_date",
        "store_name_city",
        "region_clean",
        F.when(F.col("city") == "Bangkok", 1).otherwise(0).alias("is_bangkok"),
    )
)

latest_match = (
    cleaned
    .alias("s")
    .join(df_inventory.alias("c"), (F.col("c.store_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.city",
        "s.region",
        "s.opened_date",
        "s.region_clean",
        F.col("c.quantity"),
    )
)

scored = (
    latest_match
    .select(
        "*",
        F.avg("quantity").over(w2).alias("running_avg_quantity"),
        F.min("quantity").over(w2).alias("running_min_quantity"),
        F.count("quantity").over(w2).alias("running_count_quantity"),
        F.stddev_samp("quantity").over(w3).alias("region_stddev_samp_quantity"),
    )
)

result = (
    scored
    .groupBy("city")
    .agg(
        F.count(F.when(F.col("region") == "South", 1)).alias("south_count"),
        F.round(F.sum("region_stddev_samp_quantity") / F.nullif(F.count(F.when(F.col("city") == "New York", 1)), F.lit(0)), 2).alias("region_stddev_samp_quantity_per_new_york"),
        F.count(F.when(F.col("region") == "Central", 1)).alias("central_count"),
    )
)
