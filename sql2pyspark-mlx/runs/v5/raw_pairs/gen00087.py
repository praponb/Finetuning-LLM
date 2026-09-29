from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.sale_date").desc(), F.col("c.id").desc())
w2 = Window.partitionBy("city").orderBy(F.col("sqft").desc())

latest_match = (
    df_stores
    .alias("s")
    .join(df_store_sales.alias("c"), (F.col("c.store_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.revenue").alias("store_sale_revenue"))
)

ranked = (
    latest_match
    .withColumn("dedup_rank", F.rank().over(w2))
    .filter(F.col("dedup_rank") == 1)
)

matched_keys = (
    ranked
    .select("region", "city")
    .subtract(
        df_stores
        .filter(F.col("city") == "Berlin")
        .select("region", "city")
    )
)

kept_rows = (
    ranked
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.region") == F.col("k.region")) & (F.col("s.city") == F.col("k.city")), "inner")
    .select("s.*")
)

not_flagged = (
    kept_rows
    .alias("s")
    .join(df_inventory.alias("c"), (F.col("c.store_id") == F.col("s.id")) & (F.col("c.quantity") == 183), "left_anti")
)

eligible = (
    not_flagged
    .alias("s")
    .join(df_store_sales.alias("c"), (F.col("c.store_id") == F.col("s.id")) & (F.col("c.store_id").isin([3, 4, 5])), "left_semi")
)

stats = (
    eligible
    .groupBy(F.date_trunc("YEAR", "opened_date").alias("year_start"))
    .agg(
        F.sum(F.when(F.col("city") == "New York", F.col("store_sale_revenue")).otherwise(0)).alias("new_york_store_sale_revenue"),
        F.round(F.avg("sqft"), 2).alias("avg_sqft"),
        F.round(F.sum("sqft") / F.nullif(F.count(F.when(F.col("city") == "New York", 1)), F.lit(0)), 2).alias("sqft_per_new_york"),
    )
    .filter(F.col("sqft_per_new_york") > 0)
)

result = (
    stats
    .select(
        "year_start",
        "new_york_store_sale_revenue",
        "avg_sqft",
        "sqft_per_new_york",
    )
    .orderBy("year_start", "sqft_per_new_york")
)
