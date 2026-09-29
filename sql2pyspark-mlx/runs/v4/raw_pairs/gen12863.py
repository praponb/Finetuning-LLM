from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.orderBy("started_at", "id")
w3 = Window.partitionBy("country").orderBy("started_at", "id")

filtered = (
    df_sessions
    .filter(F.to_date("started_at") < "2024-08-01")
    .select(
        "*",
        F.when(F.col("country").isin(["SG", "US"]), "Core").otherwise("Other").alias("country_group"),
        F.coalesce("browser", F.lit("NONE")).alias("browser_clean"),
    )
)

metrics = (
    filtered
    .select(
        "*",
        F.first("browser", ignorenulls=True).over(w).alias("first_known_browser"),
        F.row_number().over(w2).alias("event_seq"),
    )
)

with_gaps = (
    metrics
    .withColumn("is_new_burst", F.when(F.lag("started_at").over(w3).isNull() | (F.col("started_at") > F.lag("started_at").over(w3) + F.expr("INTERVAL 90 DAYS")), 1).otherwise(0))
)

islands = with_gaps.withColumn("island_id", F.sum("is_new_burst").over(w))

matched_keys = (
    islands
    .select("device", "country")
    .intersectAll(
        df_sessions
        .filter(F.col("country").isin(["TH", "US", "DE"]))
        .select("device", "country")
    )
)

kept_rows = (
    islands
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.device") == F.col("k.device")) & (F.col("s.country") == F.col("k.country")), "inner")
    .select("s.*")
)

valid = (
    kept_rows
    .filter((F.col("started_at") >= F.lit("2023-12-08").cast("date")) & F.col("browser").isNotNull())
    .withColumn("is_tablet", F.when(F.col("device") == "tablet", 1).otherwise(0))
)

result = (
    valid
    .rollup("browser")
    .agg(
        F.round(F.sum("island_id") / F.nullif(F.count(F.when(F.col("browser") == "Safari", 1)), F.lit(0)), 2).alias("island_id_per_safari"),
        F.sum(F.when(F.col("browser") == "Chrome", F.col("is_new_burst")).otherwise(0)).alias("chrome_is_new_burst"),
        F.percentile_approx("is_new_burst", [0.25, 0.5, 0.75]).alias("is_new_burst_quartiles"),
    )
    .filter(F.col("chrome_is_new_burst") > 0)
    .orderBy(F.col("browser").asc_nulls_last())
)
