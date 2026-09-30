from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("region", "city").orderBy(F.col("sqft").desc(), "id")
w2 = Window.partitionBy("city").orderBy(F.col("latest_rank").desc(), "id")
w3 = Window.partitionBy("region").orderBy("opened_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("city").orderBy(F.col("running_avg_sqft").desc_nulls_last(), "id")

latest = (
    df_stores
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
)

ranked = (
    latest
    .withColumn("row_num", F.row_number().over(w2))
    .filter(F.col("row_num") == 1)
    .select("id", "city", "region", "opened_date", "sqft", "latest_rank")
)

scored = (
    ranked
    .select(
        "id",
        "city",
        "opened_date",
        "latest_rank",
        F.max("sqft").over(w3).alias("running_max_sqft"),
        F.avg("sqft").over(w3).alias("running_avg_sqft"),
        F.min("sqft").over(w3).alias("running_min_sqft"),
        F.count("sqft").over(w3).alias("running_count_sqft"),
    )
)

latest_stores = (
    scored
    .withColumn("rn", F.row_number().over(w4))
    .filter(F.col("rn") == 1)
    .select("id", "latest_rank", "running_avg_sqft", "running_count_sqft")
)

kept_keys = (
    latest_stores
    .select("id")
    .exceptAll(
        df_stores
        .filter(~F.col("city").isin(["Bangkok", "Chiang Mai", "London"]))
        .select("id")
    )
)

reconciled = (
    latest_stores
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

totals = (
    reconciled
    .groupBy(
        F.when(F.col("running_count_sqft") >= 17, "PREMIUM").otherwise("STANDARD").alias("running_count_sqft_tier"),
    )
    .agg(
        F.count("*").alias("row_count"),
        F.count("*").alias("cnt"),
        F.round(F.avg("running_count_sqft"), 2).alias("avg_running_count_sqft"),
        F.countDistinct("id").alias("distinct_ids"),
        F.countDistinct("id").alias("distinct_ids_2"),
    )
    .filter(F.col("cnt") >= 1)
)

result = (
    totals
    .select("running_count_sqft_tier", "row_count", "cnt", "distinct_ids_2")
    .orderBy(F.col("cnt").asc_nulls_last())
)
