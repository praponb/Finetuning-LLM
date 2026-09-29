from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_recent = Window.partitionBy("device", "country").orderBy("started_at", "id")
w_hist = Window.partitionBy("browser").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("country").orderBy("age", "id").rowsBetween(-3, Window.currentRow)
w4 = Window.partitionBy("customer_id").orderBy("seq_no", "id")
w5 = Window.orderBy("age", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.partitionBy("customer_id").orderBy(F.col("started_at").cast("bigint")).rangeBetween(-604800, Window.currentRow)
w7 = Window.partitionBy("country").orderBy("started_at", "id")
w8 = Window.partitionBy("country").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

metrics = (
    df_sessions
    .select(
        "*",
        F.row_number().over(w_recent).alias("seq_no"),
        F.first("browser", ignorenulls=True).over(w_hist).alias("first_known_browser"),
    )
)

filtered = (
    metrics
    .filter((F.col("started_at") > F.lit("2023-03-28").cast("date")) & F.col("customer_id").isNotNull())
    .withColumn("country_length", F.length("country"))
    .withColumn("seq_no_rounded", F.round("seq_no", 0))
)

joined = (
    filtered
    .alias("s")
    .join(df_customers.alias("b"), F.col("s.customer_id") == F.col("b.id"), "left")
    .select("s.*", F.col("b.credit_limit"), F.col("b.age"))
)

enriched = (
    joined
    .withColumn("seq_no_rounded_avg_3", F.avg("seq_no_rounded").over(w3))
    .withColumn("seq_no_bucket", F.ntile(4).over(w4))
    .withColumn("last_known_country_length", F.last("country_length", ignorenulls=True).over(w5))
    .withColumn("credit_limit_sum_7d", F.sum("credit_limit").over(w6))
)

with_gaps = (
    enriched
    .withColumn("is_new_burst", F.when(F.lag("started_at").over(w7).isNull() | (F.col("started_at") > F.lag("started_at").over(w7) + F.expr("INTERVAL 60 DAYS")), 1).otherwise(0))
)

sessionized = with_gaps.withColumn("session_no", F.sum("is_new_burst").over(w8))

joined_sessions = (
    sessionized
    .alias("e")
    .join(df_customers.alias("c"), F.col("e.customer_id") == F.col("c.id"), "inner")
    .select("e.*", F.col("c.age").alias("customer_age"), F.col("c.segment"))
)

result = (
    joined_sessions
    .groupBy("segment", "browser")
    .pivot("device", ["desktop", "mobile"])
    .agg(F.sum("age"))
)
