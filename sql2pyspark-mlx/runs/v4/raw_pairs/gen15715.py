from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy(F.col("opened_date").desc(), "id")
w2 = Window.partitionBy("city").orderBy("sqft", "id")
w3 = Window.partitionBy("region").orderBy("sqft", "id")
w4 = Window.partitionBy("city", "region").orderBy("opened_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("city").orderBy(F.col("sqft").desc(), "id")

base = (
    df_stores
    .filter(F.col("region").isNotNull() & (F.col("opened_date") >= "2024-01-01"))
    .select(
        "id",
        "city",
        "region",
        "opened_date",
        "sqft",
        F.add_months("opened_date", 12).alias("opened_date_plus_months"),
        F.regexp_replace("store_name", " ", "-").alias("store_name_slug"),
    )
)

enriched = (
    base
    .select(
        "*",
        F.row_number().over(w).alias("row_seq"),
        F.ntile(4).over(w2).alias("sqft_bucket"),
        F.ntile(10).over(w3).alias("sqft_bucket_2"),
        F.last("region", ignorenulls=True).over(w4).alias("last_known_region"),
        F.ntile(4).over(w5).alias("sqft_bucket_3"),
    )
)

kept = (
    enriched
    .alias("s")
    .join(df_inventory.alias("x"), (F.col("x.store_id") == F.col("s.id")) & (F.col("x.last_restock") >= F.col("s.opened_date_plus_months")), "left_semi")
)

result = (
    kept
    .groupBy(F.date_trunc("MONTH", "opened_date").alias("activity_month"))
    .agg(
        F.round(F.sum("sqft_bucket_2") / F.nullif(F.count(F.when(F.col("region") == "Central", 1)), F.lit(0)), 2).alias("sqft_bucket_2_per_central"),
        F.sum(F.when(F.col("region") == "Central", F.col("row_seq"))).alias("central_total_row_seq"),
        F.count_if(F.col("region") == "Central").alias("central_events"),
    )
    .orderBy(F.col("central_events").desc_nulls_first())
)
