from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy("opened_date", "id")

metrics = (
    df_stores
    .select(
        "id",
        "store_name",
        "city",
        "sqft",
        F.lag("sqft").over(w).alias("prev_sqft"),
        F.row_number().over(w).alias("seq_no"),
    )
)

kept = (
    metrics
    .alias("s")
    .join(df_store_sales.alias("x"), (F.col("x.store_id") == F.col("s.id")) & (F.col("x.revenue") > 3400), "left_anti")
)

base = (
    kept
    .filter(F.col("city") == "Bangkok")
    .select(
        "*",
        F.concat("city", F.lit(" - "), "store_name").alias("city_store_name"),
        F.greatest("sqft", "seq_no").alias("greatest_sqft_seq_no"),
    )
)

cleaned = (
    base
    .filter(F.col("greatest_sqft_seq_no") <= 60)
    .select(
        "id",
        "store_name",
        "city",
        "sqft",
        "seq_no",
        "greatest_sqft_seq_no",
        (F.col("prev_sqft") / F.nullif("greatest_sqft_seq_no", F.lit(0))).alias("prev_sqft_per_greatest_sqft_seq_no"),
        (F.col("sqft") - F.col("prev_sqft")).alias("sqft_minus_prev_sqft"),
    )
)

result = (
    cleaned
    .groupBy("seq_no", "city")
    .agg(
        F.round(F.sum("sqft") / F.nullif(F.count(F.when(F.col("city") == "Singapore", 1)), F.lit(0)), 2).alias("sqft_per_singapore"),
        F.bit_or("sqft").alias("sqft_bit_or"),
        F.sum(F.when(F.col("city") == "Tokyo", F.col("seq_no")).otherwise(0)).alias("tokyo_seq_no"),
        F.round(F.avg("greatest_sqft_seq_no"), 2).alias("avg_greatest_sqft_seq_no"),
        F.percentile_approx("sqft_minus_prev_sqft", 0.9).alias("sqft_minus_prev_sqft_p90"),
        F.sum(F.when(F.col("city") == "Berlin", F.col("greatest_sqft_seq_no"))).alias("berlin_total_greatest_sqft_seq_no"),
    )
    .filter(F.col("sqft_bit_or") > 1)
    .orderBy(F.col("tokyo_seq_no").desc())
)
