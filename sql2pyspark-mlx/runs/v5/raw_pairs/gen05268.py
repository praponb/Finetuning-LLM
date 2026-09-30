from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city", "region").orderBy("opened_date", "id").rowsBetween(-5, -1)
w2 = Window.partitionBy("city").orderBy("sqft", "id").rowsBetween(-6, Window.currentRow)
w3 = Window.partitionBy("region", "city").orderBy("opened_date", "id").rowsBetween(-3, -1)
w_hist = Window.partitionBy("city", "region").orderBy("sqft_avg_5_prior", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("region").orderBy("sqft_avg_6", "id")

base = (
    df_stores
    .filter(F.col("region").isNotNull() & (F.col("region").eqNullSafe(F.lit("North"))))
    .select(
        "*",
        F.trim("store_name").alias("clean_store_name"),
        F.date_format("opened_date", "MMM yyyy").alias("opened_date_label"),
    )
)

scored = (
    base
    .filter(F.month("opened_date").isin([1, 6, 8]))
    .withColumn("sqft_avg_5_prior", F.avg("sqft").over(w))
    .withColumn("sqft_avg_6", F.avg("sqft").over(w2))
    .withColumn("sqft_avg_3_prior", F.avg("sqft").over(w3))
)

enriched = (
    scored
    .select(
        "id",
        "store_name",
        "region",
        "sqft",
        "opened_date_label",
        "sqft_avg_3_prior",
        F.avg("sqft").over(w_hist).alias("running_avg_sqft"),
        F.sum("sqft").over(w_hist).alias("running_sum_sqft"),
        F.min("sqft").over(w_hist).alias("running_min_sqft"),
        F.ntile(10).over(w5).alias("sqft_avg_6_bucket"),
    )
)

kept_keys = (
    enriched
    .select("id", "region")
    .subtract(
        df_stores
        .filter(F.col("city") == "New York")
        .select("id", "region")
    )
)

reconciled = (
    enriched
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.region") == F.col("k.region")), "inner")
    .select("s.*")
)

result = (
    reconciled
    .groupBy("region")
    .agg(
        F.sum(F.when(F.col("region") == "East", F.col("running_avg_sqft")).otherwise(0)).alias("east_running_avg_sqft"),
        F.avg("sqft").alias("avg_sqft"),
    )
    .filter(F.col("east_running_avg_sqft") > 0)
    .orderBy(F.col("east_running_avg_sqft").desc())
)
