from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country").orderBy("started_at_period", "id")
w2 = Window.partitionBy("country").orderBy("started_at_period", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

valid = (
    df_sessions
    .filter((F.col("started_at") > F.to_date(F.lit("2023-06-26"))) & (F.col("customer_id") == 40))
    .select(
        "id",
        "device",
        "country",
        F.date_trunc("year", "started_at").alias("started_at_period"),
        F.to_date("started_at").alias("started_at_date"),
    )
)

prev_events = valid.withColumn("prev_started_at_period", F.lag("started_at_period").over(w))

with_gaps = (
    prev_events
    .withColumn("is_new_burst", F.when(F.col("prev_started_at_period").isNull() | (F.col("started_at_period") > F.col("prev_started_at_period") + F.expr("INTERVAL 90 DAYS")), 1).otherwise(0))
)

islands = with_gaps.withColumn("island_id", F.sum("is_new_burst").over(w2))

result = (
    islands
    .groupBy(F.date_trunc("YEAR", "started_at_period").alias("year_start"))
    .agg(
        F.count_if(F.col("country") == "US").alias("us_events"),
        F.sort_array(F.collect_set("country")).alias("country_list"),
        F.count(F.when(F.col("country") == "DE", 1)).alias("de_rows"),
        ((F.unix_timestamp(F.max("started_at_period")) - F.unix_timestamp(F.min("started_at_period"))) / 60).alias("duration_min"),
    )
    .orderBy(F.col("year_start").desc_nulls_first())
)
