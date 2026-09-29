from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("sqft").asc_nulls_first())
w2 = Window.orderBy("opened_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

cleaned = (
    df_stores
    .filter(F.col("opened_date") > F.to_date(F.lit("2024-01-21")))
    .select(
        "id",
        "store_name",
        "opened_date",
        "sqft",
        F.round((F.col("sqft") * 1.1), 2).alias("adjusted_sqft"),
        F.trim("region").alias("clean_region"),
        F.coalesce("region", F.lit("UNKNOWN")).alias("resolved_region"),
    )
)

not_flagged = (
    cleaned
    .alias("s")
    .join(df_orders.alias("ch"), (F.col("ch.store_id") == F.col("s.id")) & (F.col("ch.shipping_fee") > 32), "left_anti")
)

metrics = (
    not_flagged
    .withColumn("sqft_percent_rank", F.percent_rank().over(w))
    .withColumn("running_adjusted_sqft", F.sum("adjusted_sqft").over(w2))
)

kept_keys = (
    metrics
    .select("id")
    .subtract(
        df_stores
        .filter(F.col("sqft") != 6377)
        .select("id")
    )
)

kept_rows = (
    metrics
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

not_flagged_stores = (
    kept_rows
    .alias("s")
    .join(df_store_sales.alias("c"), (F.col("c.store_id") == F.col("s.id")) & (F.col("c.sale_date") < F.col("s.opened_date")), "left_semi")
)

summary = (
    not_flagged_stores
    .groupBy(F.date_trunc("WEEK", "opened_date").alias("week_start"))
    .agg(
        F.percentile_approx("adjusted_sqft", [0.25, 0.5, 0.75]).alias("adjusted_sqft_quartiles"),
        F.round(F.avg("sqft"), 2).alias("avg_sqft"),
        F.percentile_approx("sqft_percent_rank", [0.25, 0.5, 0.75]).alias("sqft_percent_rank_quartiles"),
    )
    .filter(F.col("avg_sqft") < 0)
)

result = (
    summary
    .select("week_start", "adjusted_sqft_quartiles", "avg_sqft")
    .orderBy(F.col("avg_sqft").desc_nulls_last())
)
