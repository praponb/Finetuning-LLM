from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("region").orderBy("sqft", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("region").orderBy("sqft", "id")
w3 = Window.partitionBy("region", "city").orderBy("sqft", "id")
w4 = Window.partitionBy("city").orderBy(F.col("sqft").desc(), "id")

filtered = (
    df_stores
    .filter(F.upper("store_name").like("%IVE%"))
    .select(
        "*",
        F.upper(F.trim("store_name")).alias("store_name_normalized"),
        F.coalesce("region", F.lit("N/A")).alias("region_filled"),
    )
)

with_windows = (
    filtered
    .select(
        "*",
        F.last("region", ignorenulls=True).over(w).alias("last_known_region"),
        F.lag("sqft", 1, 0).over(w2).alias("prev_sqft"),
        F.lag("sqft", 1).over(w3).alias("prev_sqft_2"),
        F.row_number().over(w4).alias("seq_no"),
    )
)

base = (
    with_windows
    .filter(F.col("region").isNotNull() & (F.col("city") != "Singapore"))
    .select(
        "*",
        (F.col("sqft") - F.col("seq_no")).alias("sqft_minus_seq_no"),
        F.date_format("opened_date", "yyyy-MM-dd").alias("opened_date_label"),
    )
)

totals = (
    base
    .withColumn("sqft_minus_seq_no_tier", F.when(F.col("sqft_minus_seq_no") >= 79, "HIGH").otherwise("LOW"))
    .cube("sqft_minus_seq_no_tier")
    .agg(
        F.count("*").alias("row_count"),
        F.count(F.when(F.col("city") == "Tokyo", 1)).alias("tokyo_rows"),
        F.sum(F.when(F.col("region") == "Central", F.col("sqft")).otherwise(0)).alias("central_sqft"),
        F.bit_and("sqft").alias("sqft_bit_and"),
        F.grouping_id().alias("grouping_id"),
    )
)

result = (
    totals
    .select("row_count", "tokyo_rows", "central_sqft", "sqft_bit_and")
    .orderBy(F.col("central_sqft").asc_nulls_last(), F.col("sqft_bit_and").desc())
)
