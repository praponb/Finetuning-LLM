from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country").orderBy(F.col("credit_limit").desc(), "id")

latest = (
    df_customers
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
    .select("id", "country", "segment", "credit_limit", "age")
)

eligible = (
    latest
    .alias("s")
    .join(df_orders.alias("ch"), (F.col("ch.customer_id") == F.col("s.id")) & (F.month(F.col("ch.order_date")).isin([3, 8, 10])), "left_anti")
)

filtered = (
    eligible
    .filter(F.col("segment").isNotNull() & (F.col("segment").eqNullSafe(F.lit("Consumer"))))
    .select(
        "id",
        "country",
        "segment",
        "credit_limit",
        "age",
        F.ceil("age").alias("age_whole"),
        F.coalesce("segment", F.lit("UNKNOWN")).alias("segment_filled"),
    )
)

valid = (
    filtered
    .filter(F.col("segment").isNotNull())
    .select(
        "id",
        "segment",
        "age",
        "age_whole",
        "segment_filled",
        (F.col("credit_limit") * F.col("age")).alias("credit_limit_times_age"),
        F.concat("country", F.lit(" "), "segment").alias("country_segment"),
    )
)

kept = (
    valid
    .alias("s")
    .join(df_sessions.alias("x"), (F.col("x.customer_id") == F.col("s.id")) & (F.col("x.device") == "desktop"), "left_anti")
)

eligible_customers = (
    kept
    .alias("s")
    .join(df_sessions.alias("ch"), (F.col("ch.customer_id") == F.col("s.id")) & (F.col("ch.customer_id") == 31), "left_semi")
)

kept_customers = (
    eligible_customers
    .alias("s")
    .join(df_sessions.alias("c"), (F.col("c.customer_id") == F.col("s.id")) & (~F.col("c.device").isin(["tablet", "desktop"])), "left_anti")
)

stats = (
    kept_customers
    .groupBy("segment")
    .agg(
        F.sum(F.when(F.col("segment") == "Corporate", F.col("age")).otherwise(0)).alias("corporate_age"),
        F.size(F.collect_set("segment")).alias("num_segments"),
        F.size(F.collect_set("segment")).alias("num_segments_2"),
    )
)

result = stats.select("segment", "corporate_age")
