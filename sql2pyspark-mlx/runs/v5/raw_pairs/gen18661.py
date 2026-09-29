from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_grp = Window.orderBy(F.col("sqft").desc(), "id")
w_ordered = Window.orderBy("sqft", "id")
w3 = Window.partitionBy("region", "city")
w4 = Window.partitionBy("city").orderBy("opened_date_period", "id")
w5 = Window.partitionBy("city").orderBy("opened_date_period", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.partitionBy("city").orderBy(F.col("region_avg_sqft").desc(), "id")

valid = (
    df_stores
    .filter(~F.col("region").isin(["East", "North"]) & (F.col("region").eqNullSafe(F.lit("North"))))
    .withColumn("opened_date_period", F.date_trunc("year", "opened_date"))
    .withColumn("region_filled", F.coalesce("region", F.lit("OTHER")))
)

scored = (
    valid
    .select(
        "id",
        "store_name",
        "city",
        "sqft",
        "opened_date_period",
        "region_filled",
        F.ntile(4).over(w_grp).alias("sqft_bucket"),
        F.row_number().over(w_ordered).alias("row_seq"),
        F.avg("sqft").over(w3).alias("region_avg_sqft"),
    )
)

prev_events = scored.withColumn("prev_opened_date_period", F.lag("opened_date_period").over(w4))

flagged = (
    prev_events
    .withColumn("session_start_flag", F.when(F.col("prev_opened_date_period").isNull() | (F.col("opened_date_period") > F.col("prev_opened_date_period") + F.expr("INTERVAL 7 DAYS")), 1).otherwise(0))
)

sessions_stores = flagged.withColumn("island_id", F.sum("session_start_flag").over(w5))

latest = (
    sessions_stores
    .withColumn("latest_rank", F.row_number().over(w6))
    .filter(F.col("latest_rank") == 1)
)

result = (
    latest
    .groupBy("city", "island_id")
    .agg(
        F.count(F.when(F.col("city") == "London", 1)).alias("london_count"),
        F.sum(F.when(F.col("city") == "London", F.col("sqft_bucket")).otherwise(0)).alias("london_sqft_bucket"),
        F.sum("row_seq").alias("total_row_seq"),
    )
    .filter(F.col("london_sqft_bucket") < 1000000)
    .orderBy("total_row_seq")
)
