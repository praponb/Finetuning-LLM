from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy(F.col("age").desc())

filtered = (
    df_customers
    .filter(F.col("segment").isNotNull())
    .select("*", F.coalesce("segment", F.lit("N/A")).alias("segment_filled"))
)

eligible = (
    filtered
    .alias("s")
    .join(df_sessions.alias("c"), (F.col("c.customer_id") == F.col("s.id")) & (F.col("c.device") != "mobile"), "left_anti")
)

ranked = (
    eligible
    .withColumn("dedup_rank", F.rank().over(w))
    .filter(F.col("dedup_rank") == 1)
    .select("id", "name", "email", "is_active", "age", "segment_filled")
)

kept = (
    ranked
    .alias("s")
    .join(df_sessions.alias("ch"), (F.col("ch.customer_id") == F.col("s.id")) & (F.col("ch.browser").isin(["Safari", "Chrome", "Edge"])), "left_anti")
)

result = (
    kept
    .groupBy(F.when(F.col("age") >= 63, "HIGH").otherwise("LOW").alias("age_tier"))
    .agg(
        F.avg("age").alias("avg_age"),
        F.round(F.avg("age"), 2).alias("avg_age_2"),
        F.percentile_approx("age", 0.5).alias("age_p50"),
    )
    .filter(F.col("avg_age_2") > 0)
    .orderBy(F.col("age_p50").desc())
)
