from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("started_at_hour")

valid = (
    df_sessions
    .filter(~F.col("country").eqNullSafe(F.lit("DE")))
    .select(
        "*",
        F.hour("started_at").alias("started_at_hour"),
        F.upper("browser").alias("browser_upper"),
    )
)

kept_keys = (
    valid
    .select("customer_id", "id")
    .intersectAll(
        df_sessions
        .filter(F.col("device") == "tablet")
        .select("customer_id", "id")
    )
)

verified = (
    valid
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.customer_id") == F.col("k.customer_id")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

enriched = (
    verified
    .alias("a")
    .join(df_customers.alias("b"), F.col("a.customer_id").eqNullSafe(F.col("b.id")), "inner")
    .filter(F.col("b.country") == "SG")
    .select(
        "a.customer_id",
        "a.device",
        "a.country",
        "a.started_at_hour",
        F.col("b.city"),
    )
)

scored = enriched.withColumn("started_at_hour_dense_rank", F.dense_rank().over(w))

result = (
    scored
    .groupBy("country")
    .agg(
        F.count_if(F.col("device") == "mobile").alias("mobile_events"),
        F.round(F.avg("started_at_hour_dense_rank"), 2).alias("avg_started_at_hour_dense_rank"),
        F.count(F.when(F.col("country") == "US", 1)).alias("us_rows"),
        F.max(F.when(F.col("city") == "Tokyo", F.col("started_at_hour_dense_rank"))).alias("tokyo_max_started_at_hour_dense_rank"),
    )
    .orderBy(F.col("country").desc())
)
