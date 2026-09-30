from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy(F.col("units").asc_nulls_last())
w2 = Window.partitionBy("store_id", "product_id").orderBy(F.col("sale_date").desc(), "id")
w3 = Window.partitionBy("store_id")

with_windows = (
    df_store_sales
    .select(
        "*",
        F.percent_rank().over(w).alias("units_percent_rank"),
        F.row_number().over(w2).alias("row_seq"),
        F.avg("revenue").over(w3).alias("store_id_avg_revenue"),
    )
)

cleaned = (
    with_windows
    .filter(F.col("sale_date") > F.lit("2024-02-22").cast("date"))
    .select(
        "id",
        "store_id",
        "units",
        "revenue",
        "units_percent_rank",
        "row_seq",
        "store_id_avg_revenue",
        F.when(F.col("units") >= 41, "High").when(F.col("units") >= 15, "Medium").otherwise("Low").alias("units_level"),
    )
)

matched_keys = (
    cleaned
    .select("store_id")
    .intersect(
        df_store_sales
        .filter(F.col("units") < 37)
        .select("store_id")
    )
)

verified = (
    cleaned
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.store_id") == F.col("k.store_id")), "inner")
    .select("s.*")
)

result = (
    verified
    .groupBy(
        F.when(F.col("row_seq") >= 9, "HIGH").otherwise("LOW").alias("row_seq_tier"),
        "store_id",
    )
    .agg(
        F.percentile_approx("row_seq", 0.95).alias("row_seq_p95"),
        F.percentile_approx("units_percent_rank", [0.25, 0.5, 0.75]).alias("units_percent_rank_quartiles"),
    )
    .orderBy(F.col("store_id").desc_nulls_last())
)
