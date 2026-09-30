from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("region", "city").orderBy("sqft", "id")
w2 = Window.partitionBy("city").orderBy("sqft", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("city", "region").orderBy("opened_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.orderBy("sqft", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("region", "city").orderBy("opened_date", "id")
w6 = Window.partitionBy("region").orderBy(F.col("sqft").desc(), "id")

valid = (
    df_stores
    .filter(F.col("opened_date") > F.lit("2023-06-18").cast("date"))
    .withColumn("sqft_rounded", F.round("sqft", 0))
    .withColumn("region_clean", F.coalesce("region", F.lit("DIRECT")))
)

ranked = (
    valid
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
)

enriched = (
    ranked
    .withColumn("running_sqft_rounded", F.sum("sqft_rounded").over(w2))
    .withColumn("cumulative_sqft_rounded", F.sum("sqft_rounded").over(w3))
    .withColumn("cumulative_sqft_rounded_2", F.sum("sqft_rounded").over(w4))
    .withColumn("prev_sqft", F.lag("sqft", 2, 0).over(w5))
    .withColumn("sqft_bucket", F.ntile(4).over(w6))
)

valid_stores = (
    enriched
    .filter(F.col("region").isNotNull())
    .select(
        "id",
        "store_name",
        "city",
        "region",
        "opened_date",
        "sqft",
        "sqft_rounded",
        "region_clean",
        "dedup_rank",
        "cumulative_sqft_rounded_2",
        "prev_sqft",
        "sqft_bucket",
        F.upper("store_name").alias("store_name_upper"),
        (F.col("sqft") * F.col("sqft_rounded")).alias("sqft_times_sqft_rounded"),
        F.coalesce("region", F.lit("N/A")).alias("resolved_region"),
    )
)

latest = (
    valid_stores
    .withColumn("rank_in_group", F.row_number().over(w5))
    .filter(F.col("rank_in_group") == 1)
)

result = (
    latest
    .select(
        "id",
        "region",
        F.col("sqft_times_sqft_rounded").alias("SQFT_TIMES_SQFT_ROUNDED_VAL"),
        F.col("sqft_rounded").alias("SQFT_ROUNDED"),
    )
    .unpivot(
        ["id", "region"],
        ["SQFT_TIMES_SQFT_ROUNDED_VAL", "SQFT_ROUNDED"],
        "metric_name",
        "reading",
    )
)
