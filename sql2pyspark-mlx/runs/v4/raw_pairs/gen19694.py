from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("browser").orderBy(F.col("credit_limit").desc(), "id")
w2 = Window.partitionBy("device").orderBy(F.col("started_at").cast("bigint")).rangeBetween(-604800, Window.currentRow)
w3 = Window.orderBy("started_at", "id").rowsBetween(-2, Window.currentRow)
w4 = Window.partitionBy("device").orderBy("started_at", "id")

joined = (
    df_sessions
    .alias("src")
    .join(df_customers.alias("d"), F.col("src.customer_id") == F.col("d.id"), "inner")
    .select(
        "src.*",
        F.col("d.credit_limit"),
        F.col("d.name"),
        F.col("d.country").alias("customer_country"),
    )
)

kept = (
    joined
    .alias("s")
    .join(df_web_events.alias("x"), (F.col("x.session_id") == F.col("s.id")) & (F.col("x.event_time") >= F.col("s.started_at")), "left_semi")
)

metrics = (
    kept
    .select(
        "*",
        F.ntile(3).over(w).alias("credit_limit_bucket"),
        F.count("credit_limit").over(w2).alias("credit_limit_count_7d"),
        F.avg("credit_limit").over(w3).alias("credit_limit_avg_2"),
        F.lag("credit_limit", 1).over(w4).alias("prev_credit_limit"),
    )
)

result = (
    metrics
    .groupBy(
        F.when(F.col("credit_limit_bucket") >= 9, "HIGH").otherwise("LOW").alias("credit_limit_bucket_tier"),
        "customer_country",
    )
    .agg(
        F.min("started_at").alias("first_seen"),
        F.round(F.sum("credit_limit") / F.nullif(F.count(F.when(F.col("customer_country") == "UK", 1)), F.lit(0)), 2).alias("credit_limit_per_uk"),
        F.countDistinct("country").alias("distinct_countries"),
        F.count(F.when(F.col("customer_country") == "JP", 1)).alias("jp_count"),
        F.count(F.when(F.col("device") == "mobile", 1)).alias("mobile_rows"),
        F.sort_array(F.collect_set("device")).alias("device_list"),
    )
    .orderBy(F.col("jp_count").asc_nulls_last())
)
