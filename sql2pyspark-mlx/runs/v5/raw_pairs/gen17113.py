from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy("created_at", "id", "attr_key").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.orderBy(F.col("created_at").desc(), "id", "attr_key")
w3 = Window.partitionBy("attr_key").orderBy("created_at", "id", "attr_key")
w_grp = Window.partitionBy("attr_key").orderBy("created_at", "id", "attr_key").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("attr_key").orderBy("row_seq", "id", "attr_key").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_running = Window.partitionBy("attr_key").orderBy("created_at", "id", "attr_key").rowsBetween(-3, Window.currentRow)
w7 = Window.orderBy("rn", "id", "attr_key").rowsBetween(Window.unboundedPreceding, Window.currentRow)

lines = (
    df_carts
    .select(
        "id",
        "customer_id",
        "created_at",
        "coupon_codes",
        F.explode("attributes").alias("attr_key", "v"),
    )
)

metrics = (
    lines
    .filter(F.array_contains("coupon_codes", "SPRING10"))
    .select(
        "id",
        "created_at",
        "coupon_codes",
        "attr_key",
        F.first("attr_key", ignorenulls=True).over(w).alias("first_known_attr_key"),
        F.row_number().over(w2).alias("row_seq"),
    )
)

ranked = (
    metrics
    .withColumn("rn", F.row_number().over(w3))
    .filter(F.col("rn") == 1)
)

with_windows = (
    ranked
    .withColumn("first_known_attr_key_2", F.first("attr_key", ignorenulls=True).over(w_grp))
    .withColumn("cumulative_row_seq", F.sum("row_seq").over(w5))
    .withColumn("row_seq_avg_3", F.avg("row_seq").over(w_running))
)

with_windows_carts = (
    with_windows
    .select(
        "*",
        F.max("row_seq").over(w5).alias("running_max_row_seq"),
        F.sum("row_seq").over(w5).alias("running_sum_row_seq"),
        F.avg("row_seq").over(w5).alias("running_avg_row_seq"),
        F.sum("row_seq_avg_3").over(w7).alias("cumulative_row_seq_avg_3"),
    )
)

result = (
    with_windows_carts
    .groupBy(F.date_trunc("HOUR", "created_at").alias("hour_window"))
    .agg(
        F.count("*").alias("row_count"),
        F.round(F.sum("running_avg_row_seq") / F.nullif(F.count(F.when(F.col("attr_key") == "utm_source", 1)), F.lit(0)), 2).alias("running_avg_row_seq_per_utm_source"),
        F.round(F.avg("running_max_row_seq"), 2).alias("avg_running_max_row_seq"),
        F.sum(F.when(F.col("attr_key") == "weight_kg", F.col("cumulative_row_seq")).otherwise(0)).alias("weight_kg_cumulative_row_seq"),
        F.avg(F.when(F.col("attr_key") == "weight_kg", F.col("row_seq"))).alias("weight_kg_avg_row_seq"),
        F.count(F.when(F.col("attr_key") == "weight_kg", 1)).alias("weight_kg_rows"),
    )
)
