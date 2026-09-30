from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country").orderBy(F.col("is_desktop").desc())
w2 = Window.partitionBy("country").orderBy("dedup_rank", "id").rowsBetween(-6, Window.currentRow)
w3 = Window.partitionBy("country").orderBy("dedup_rank", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

filtered = (
    df_sessions
    .filter((~F.col("country").eqNullSafe(F.lit("US"))) & F.col("customer_id").isin([2, 26, 28]))
    .select(
        "id",
        "country",
        F.when(F.col("device") == "desktop", 1).otherwise(0).alias("is_desktop"),
        F.coalesce("browser", F.lit("NONE")).alias("resolved_browser"),
    )
)

deduped = (
    filtered
    .withColumn("dedup_rank", F.rank().over(w))
    .filter(F.col("dedup_rank") == 1)
)

scored = (
    deduped
    .filter(F.col("dedup_rank") == 1)
    .select(
        "*",
        F.avg("is_desktop").over(w2).alias("last7_avg_is_desktop"),
        F.sum("is_desktop").over(w2).alias("last7_sum_is_desktop"),
        F.max("is_desktop").over(w2).alias("last7_max_is_desktop"),
        F.last("is_desktop", ignorenulls=True).over(w3).alias("last_known_is_desktop"),
    )
)

kept = (
    scored
    .alias("s")
    .join(df_web_events.alias("c"), (F.col("c.session_id") == F.col("s.id")) & (F.col("c.session_id").isin([27, 32, 36])), "left_anti")
)

result = (
    kept
    .select(
        "id",
        "country",
        "resolved_browser",
        "dedup_rank",
        "last7_sum_is_desktop",
        "last7_max_is_desktop",
    )
    .orderBy(F.col("dedup_rank").desc(), F.col("last7_sum_is_desktop").desc())
)
