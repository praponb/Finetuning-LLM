from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("sqft").desc(), "id")
w2 = Window.orderBy("sqft_rounded_per_sqft", "id").rowsBetween(-3, -1)
w3 = Window.orderBy("sqft", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.orderBy("opened_date", "id").rowsBetween(-5, Window.currentRow)
w5 = Window.orderBy("sqft_rounded_per_sqft", "id").rowsBetween(-3, Window.currentRow)
w6 = Window.orderBy("sqft_rounded_per_sqft", "id")
w7 = Window.orderBy(F.col("sqft_bucket").desc(), "id")
w8 = Window.orderBy("sqft_rounded_avg_3_prior_rounded", "id")
w9 = Window.orderBy("sqft_rounded_per_sqft_bucket", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_ordered = Window.orderBy("opened_date", "id")
w11 = Window.orderBy("opened_date", "id").rowsBetween(-3, -1)
w12 = Window.orderBy("sqft_bucket", "id").rowsBetween(-5, Window.currentRow)

valid = (
    df_stores
    .filter((F.col("opened_date") > F.lit("2024-01-04").cast("date")) & F.col("region").isNotNull())
    .select(
        "id",
        "store_name",
        "opened_date",
        "sqft",
        F.initcap(F.trim("region")).alias("region_title"),
        F.round("sqft", 2).alias("sqft_rounded"),
    )
)

filtered = (
    valid
    .filter((F.col("opened_date") > F.lit("2023-08-23").cast("date")) & (F.col("sqft") > 8099))
    .withColumn("sqft_rounded_per_sqft", F.round((F.col("sqft_rounded") / F.col("sqft")), 2))
)

scored = (
    filtered
    .select(
        "*",
        F.ntile(10).over(w).alias("sqft_bucket"),
        F.avg("sqft_rounded").over(w2).alias("sqft_rounded_avg_3_prior"),
        F.last("sqft_rounded_per_sqft", ignorenulls=True).over(w3).alias("last_known_sqft_rounded_per_sqft"),
        F.avg("sqft_rounded").over(w4).alias("sqft_rounded_avg_5"),
    )
)

valid_stores = (
    scored
    .filter(F.col("opened_date") >= F.lit("2023-10-08").cast("date"))
    .select(
        "id",
        "store_name",
        "opened_date",
        "sqft",
        "region_title",
        "sqft_rounded_per_sqft",
        "sqft_bucket",
        F.round("sqft_rounded_avg_3_prior", 2).alias("sqft_rounded_avg_3_prior_rounded"),
    )
)

scored_stores = (
    valid_stores
    .withColumn("sqft_bucket_avg_3", F.avg("sqft_bucket").over(w5))
    .withColumn("sqft_rounded_per_sqft_bucket", F.ntile(3).over(w6))
    .withColumn("sqft_bucket_bucket", F.ntile(3).over(w7))
)

enriched = (
    scored_stores
    .select(
        "*",
        F.lead("sqft_rounded_per_sqft", 1, 0).over(w8).alias("next_sqft_rounded_per_sqft"),
        F.first("sqft_rounded_avg_3_prior_rounded", ignorenulls=True).over(w9).alias("first_known_sqft_rounded_avg_3_prior_rounded"),
        F.lead("sqft_bucket_avg_3", 1).over(w_ordered).alias("next_sqft_bucket_avg_3"),
        F.avg("sqft_bucket").over(w11).alias("sqft_bucket_avg_3_prior"),
        F.avg("sqft_bucket_avg_3").over(w12).alias("sqft_bucket_avg_3_avg_5"),
    )
)

valid_stores_2 = (
    enriched
    .filter(F.col("next_sqft_bucket_avg_3").between(4, 6))
    .select(
        "id",
        "store_name",
        "opened_date",
        "sqft",
        "sqft_rounded_per_sqft",
        "sqft_bucket",
        "sqft_rounded_avg_3_prior_rounded",
        "sqft_bucket_avg_3",
        "sqft_rounded_per_sqft_bucket",
        "next_sqft_bucket_avg_3",
        "sqft_bucket_avg_3_prior",
        "sqft_bucket_avg_3_avg_5",
        F.round((F.col("sqft_rounded_per_sqft_bucket") * 1.07), 2).alias("sqft_rounded_per_sqft_bucket_scaled"),
    )
)

result = (
    valid_stores_2
    .select(
        "id",
        "opened_date",
        "sqft_rounded_per_sqft",
        "sqft_rounded_avg_3_prior_rounded",
        "sqft_rounded_per_sqft_bucket",
        "next_sqft_bucket_avg_3",
        "sqft_bucket_avg_3_avg_5",
        "sqft_rounded_per_sqft_bucket_scaled",
    )
    .orderBy(F.col("sqft_rounded_per_sqft_bucket").asc_nulls_last())
)
