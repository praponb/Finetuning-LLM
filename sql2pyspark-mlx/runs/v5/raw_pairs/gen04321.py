from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id", "store_id").orderBy(F.col("quantity").desc_nulls_first())
w2 = Window.orderBy("days_since_last_restock_times_quantity", "id")
w3 = Window.orderBy("last_restock_plus_8d_plus_51d", "id").rowsBetween(-6, Window.currentRow)
w4 = Window.orderBy("days_since_last_restock_avg_6", "id").rowsBetween(-5, -1)
w_ordered = Window.orderBy("row_seq", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_grp = Window.orderBy(F.col("row_seq").desc_nulls_last())
w_part = Window.orderBy(F.col("days_since_last_restock_avg_6").desc(), "id")
w_hist = Window.orderBy("last_restock_plus_8d", "id")

ranked = (
    df_inventory
    .withColumn("rn", F.rank().over(w))
    .filter(F.col("rn") == 1)
    .select("id", "quantity", "last_restock")
)

filtered = (
    ranked
    .filter(F.col("last_restock") > F.lit("2023-08-27").cast("date"))
    .select(
        "*",
        F.date_add("last_restock", 8).alias("last_restock_plus_8d"),
        F.datediff(F.current_date(), "last_restock").alias("days_since_last_restock"),
    )
)

cleaned = (
    filtered
    .filter(F.col("last_restock") >= "2023-11-01")
    .withColumn("last_restock_plus_8d_plus_51d", F.date_add("last_restock_plus_8d", 51))
    .withColumn("days_since_last_restock_times_quantity", (F.col("days_since_last_restock") * F.col("quantity")))
)

valid = (
    cleaned
    .filter(F.col("last_restock_plus_8d").between("2023-09-30", "2024-12-29"))
    .select("*", F.date_sub("last_restock", 15).alias("last_restock_minus_15d"))
)

scored = (
    valid
    .select(
        "id",
        "quantity",
        "last_restock",
        "last_restock_plus_8d",
        "days_since_last_restock",
        "last_restock_minus_15d",
        F.row_number().over(w2).alias("row_seq"),
        F.avg("days_since_last_restock").over(w3).alias("days_since_last_restock_avg_6"),
    )
)

metrics = (
    scored
    .select(
        "*",
        F.avg("days_since_last_restock_avg_6").over(w4).alias("days_since_last_restock_avg_6_avg_5_prior"),
        F.sum("days_since_last_restock").over(w_ordered).alias("running_days_since_last_restock"),
        F.percent_rank().over(w_grp).alias("row_seq_percent_rank"),
        F.ntile(5).over(w_part).alias("days_since_last_restock_avg_6_bucket"),
        F.lag("quantity", 1).over(w_hist).alias("prev_quantity"),
    )
)

result = (
    metrics
    .groupBy(F.date_trunc("WEEK", "last_restock_minus_15d").alias("week_start"))
    .agg(
        F.percentile_approx("days_since_last_restock_avg_6_bucket", 0.95).alias("days_since_last_restock_avg_6_bucket_p95"),
        F.percentile_approx("quantity", [0.25, 0.5, 0.75]).alias("quantity_quartiles"),
    )
    .orderBy(F.col("days_since_last_restock_avg_6_bucket_p95").asc_nulls_first())
)
