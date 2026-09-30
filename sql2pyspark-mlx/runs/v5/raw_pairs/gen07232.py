from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy(F.col("sqft").desc_nulls_first(), "id")
w2 = Window.orderBy("opened_date", "id")
w3 = Window.partitionBy("city", "region").orderBy("sqft", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("region").orderBy("sqft", "id").rowsBetween(-2, Window.currentRow)
w5 = Window.orderBy(F.col("sqft").asc_nulls_last())
w6 = Window.partitionBy("region").orderBy("sqft", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w7 = Window.partitionBy("city", "region")

kept = (
    df_stores
    .alias("s")
    .join(df_orders.alias("ch"), (F.col("ch.store_id") == F.col("s.id")) & (F.col("ch.shipping_fee") >= 19), "left_anti")
)

latest = (
    kept
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
)

valid = (
    latest
    .filter(F.col("opened_date") >= F.lit("2023-06-08").cast("date"))
    .select(
        "id",
        "store_name",
        "city",
        "region",
        "opened_date",
        "sqft",
        "dedup_rank",
        F.trim("city").alias("clean_city"),
        F.lower("city").alias("city_lower"),
    )
)

scored = (
    valid
    .withColumn("next_sqft", F.lead("sqft", 2).over(w2))
    .withColumn("first_known_region", F.first("region", ignorenulls=True).over(w3))
    .withColumn("sqft_avg_2", F.avg("sqft").over(w4))
    .withColumn("sqft_percent_rank", F.percent_rank().over(w5))
    .withColumn("last_known_sqft", F.last("sqft", ignorenulls=True).over(w6))
)

kept_keys = (
    scored
    .select("region")
    .intersect(
        df_stores
        .filter(F.col("region").isin(["West", "North"]))
        .select("region")
    )
)

reconciled = (
    scored
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.region") == F.col("k.region")), "inner")
    .select("s.*")
)

not_flagged = (
    reconciled
    .alias("s")
    .join(df_orders.alias("ch"), (F.col("ch.store_id") == F.col("s.id")) & (F.col("ch.amount") < 1090), "left_anti")
)

metrics = (
    not_flagged
    .withColumn("last_known_sqft_share", F.round((F.col("last_known_sqft") / F.sum("last_known_sqft").over(w7)), 4))
)

result = (
    metrics
    .groupBy("city")
    .pivot("region", ["North", "East", "Central"])
    .agg(F.sum("sqft_avg_2").alias("total"), F.count(F.lit(1)).alias("n"))
    .select(
        "city",
        F.col("North_total").alias("north_total"),
        F.col("North_n").alias("north_n"),
        F.col("East_total").alias("east_total"),
        F.col("East_n").alias("east_n"),
        F.col("Central_total").alias("central_total"),
        F.col("Central_n").alias("central_n"),
    )
)
