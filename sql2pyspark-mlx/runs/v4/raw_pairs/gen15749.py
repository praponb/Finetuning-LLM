from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("segment").orderBy(F.col("credit_limit").desc(), "id")
w2 = Window.partitionBy("city").orderBy("credit_limit", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("country", "city")
w4 = Window.partitionBy("country").orderBy(F.col("credit_limit").desc_nulls_last(), "id")
w5 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.started_at").desc(), F.col("c.id").desc())

with_windows = (
    df_customers
    .withColumn("credit_limit_bucket", F.ntile(3).over(w))
    .withColumn("first_known_city", F.first("city", ignorenulls=True).over(w2))
    .withColumn("country_min_age", F.min("age").over(w3))
)

cleaned = (
    with_windows
    .filter((F.col("signup_date") > F.lit("2023-10-13").cast("date")) & F.col("city").isNotNull())
    .select(
        "*",
        F.upper("country").alias("country_upper"),
        F.coalesce("city", F.lit("N/A")).alias("resolved_city"),
    )
)

latest = (
    cleaned
    .withColumn("rn", F.row_number().over(w4))
    .filter(F.col("rn") == 1)
)

latest_match = (
    latest
    .alias("s")
    .join(df_sessions.alias("c"), (F.col("c.customer_id") == F.col("s.id")), "inner")
    .withColumn("rn_2", F.row_number().over(w5))
    .filter(F.col("rn_2") == 1)
    .select("s.*", F.col("c.started_at"))
)

pivoted = (
    latest_match
    .groupBy("segment", "country")
    .pivot("city", ["Singapore", "Tokyo", "London"])
    .agg(F.avg("country_min_age"))
    .select(
        "segment",
        "country",
        F.col("Singapore").alias("singapore"),
        F.col("Tokyo").alias("tokyo"),
        F.col("London").alias("london"),
    )
)

result = (
    pivoted
    .select("segment", "country", "singapore", "tokyo", "london")
    .orderBy(F.col("tokyo").desc())
)
