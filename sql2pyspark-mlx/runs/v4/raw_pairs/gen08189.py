from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device").orderBy("started_at", "id")
w2 = Window.partitionBy("device").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

valid = (
    df_sessions
    .filter((F.col("started_at") > F.lit("2023-10-30 03:00:00").cast("timestamp")) & F.col("browser").isNotNull() & ~F.col("country").isin(["UK", "JP", "US"]))
    .select(
        "id",
        "device",
        "browser",
        "started_at",
        "country",
        F.length("browser").alias("browser_length"),
    )
)

gaps = (
    valid
    .withColumn("is_new_burst", F.when(F.lag("started_at").over(w).isNull() | (F.col("started_at") > F.lag("started_at").over(w) + F.expr("INTERVAL 60 DAYS")), 1).otherwise(0))
)

sessionized = gaps.withColumn("burst_id", F.sum("is_new_burst").over(w2))

result = (
    sessionized
    .groupBy(F.date_trunc("YEAR", "started_at").alias("year_start"))
    .agg(
        F.percentile_approx("burst_id", [0.25, 0.5, 0.75]).alias("burst_id_quartiles"),
        F.count(F.when(F.col("device") == "mobile", 1)).alias("mobile_count"),
        ((F.unix_timestamp(F.max("started_at")) - F.unix_timestamp(F.min("started_at"))) / 60).alias("span_minutes"),
    )
    .orderBy(F.col("mobile_count").desc_nulls_first())
)
