from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("sqft").desc(), "id")
w2 = Window.partitionBy("region").orderBy("opened_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("city").orderBy(F.col("opened_date").desc(), "id")
w4 = Window.partitionBy("city", "region").orderBy("opened_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("region")
w6 = Window.partitionBy("region").orderBy(F.col("opened_date").asc_nulls_last(), "id")

filtered = (
    df_stores
    .filter((F.col("opened_date") > F.to_date(F.lit("2023-06-12"))) & F.col("region").isNotNull())
    .select(
        "id",
        "store_name",
        "city",
        "region",
        "opened_date",
        "sqft",
        F.col("sqft").cast("string").alias("sqft_str"),
        F.coalesce("region", F.lit("NONE")).alias("region_filled"),
    )
)

enriched = (
    filtered
    .select(
        "*",
        F.ntile(10).over(w).alias("sqft_bucket"),
        F.sum("sqft").over(w2).alias("cumulative_sqft"),
        F.row_number().over(w3).alias("event_seq"),
        F.first("sqft", ignorenulls=True).over(w4).alias("first_known_sqft"),
        F.round((F.col("sqft") / F.sum("sqft").over(w5)), 4).alias("sqft_share"),
    )
)

deduped = (
    enriched
    .withColumn("rank_in_group", F.row_number().over(w6))
    .filter(F.col("rank_in_group") == 1)
)

result = (
    deduped
    .select(
        "id",
        "store_name",
        "city",
        "region",
        "region_filled",
        "sqft_bucket",
        "cumulative_sqft",
        "first_known_sqft",
        "sqft_share",
    )
    .orderBy(F.col("first_known_sqft").desc_nulls_first())
)
