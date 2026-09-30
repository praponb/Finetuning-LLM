from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("sqft").desc(), "id")
w2 = Window.partitionBy("city").orderBy("opened_date_weekofyear", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("region", "city").orderBy(F.col("opened_date_month_end").desc(), "id")
w4 = Window.partitionBy("city").orderBy(F.col("latest_rank").desc())
w5 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.last_restock").desc(), F.col("c.id").desc())
w6 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.units").desc(), F.col("c.id").desc())

filtered = (
    df_stores
    .filter((F.col("opened_date") > F.to_date(F.lit("2023-03-14"))) & F.col("region").isNotNull() & (F.col("city") == "Tokyo"))
    .select(
        "*",
        F.last_day("opened_date").alias("opened_date_month_end"),
        F.weekofyear("opened_date").alias("opened_date_weekofyear"),
    )
)

enriched = (
    filtered
    .withColumn("sqft_bucket", F.ntile(4).over(w))
    .withColumn("first_known_sqft", F.first("sqft", ignorenulls=True).over(w2))
)

ranked = (
    enriched
    .withColumn("latest_rank", F.row_number().over(w3))
    .filter(F.col("latest_rank") == 1)
)

ranked_stores = (
    ranked
    .withColumn("dedup_rank", F.rank().over(w4))
    .filter(F.col("dedup_rank") == 1)
    .drop("dedup_rank")
)

eligible = (
    ranked_stores
    .alias("s")
    .join(df_store_sales.alias("c"), (F.col("c.store_id") == F.col("s.id")) & (F.col("c.units") != 24), "left_semi")
)

latest_match = (
    eligible
    .alias("s")
    .join(df_inventory.alias("c"), (F.col("c.store_id") == F.col("s.id")) & (F.col("c.last_restock") <= F.col("s.opened_date_month_end")), "left")
    .withColumn("rn", F.row_number().over(w5))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.last_restock").alias("inventory_last_restock"))
)

with_latest = (
    latest_match
    .alias("s")
    .join(df_store_sales.alias("c"), (F.col("c.store_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w6))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.units"))
)

result = (
    with_latest
    .withColumn("quarter_start", F.date_trunc("QUARTER", "opened_date"))
    .withColumn("week_start", F.date_trunc("WEEK", "opened_date_month_end"))
    .groupingSets(
        [["quarter_start", "week_start"], ["quarter_start"]],
        "quarter_start",
        "week_start",
    )
    .agg(
        F.sum("sqft_bucket").alias("total_sqft_bucket"),
        F.count(F.when(F.col("city") == "London", 1)).alias("london_count"),
        F.round(F.avg("sqft"), 2).alias("avg_sqft"),
        F.count(F.when(F.col("region") == "West", 1)).alias("west_count"),
        F.round(F.avg("sqft"), 2).alias("avg_sqft_2"),
        F.grouping("quarter_start").alias("grouping_level"),
    )
    .orderBy("avg_sqft")
)
