from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_running = Window.partitionBy("status", "carrier").orderBy("shipped_date", "id").rowsBetween(-6, Window.currentRow)
w2 = Window.partitionBy("status").orderBy(F.col("shipped_date").desc_nulls_last(), "id")
w3 = Window.partitionBy("order_status", "status").orderBy(F.col("cost").desc())
w4 = Window.partitionBy("order_id")
w5 = Window.partitionBy("carrier").orderBy("last7_count_cost", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

valid = (
    df_shipments
    .filter((F.col("shipped_date") >= F.lit("2023-07-13").cast("date")) & (F.col("order_id") == 37))
    .withColumn("delivered_date_label", F.date_format("delivered_date", "yyyy-MM-dd"))
    .withColumn("carrier_filled", F.coalesce("carrier", F.lit("UNKNOWN")))
)

metrics = (
    valid
    .select(
        "*",
        F.max("cost").over(w_running).alias("last7_max_cost"),
        F.count("cost").over(w_running).alias("last7_count_cost"),
        F.min("cost").over(w_running).alias("last7_min_cost"),
    )
)

joined = (
    metrics
    .alias("src")
    .join(df_orders.alias("p"), F.col("src.order_id").eqNullSafe(F.col("p.id")), "left")
    .filter(F.col("p.order_date") > "2024-06-30")
    .select("src.*", F.col("p.status").alias("order_status"), F.col("p.amount"))
)

ranked = (
    joined
    .withColumn("row_num", F.row_number().over(w2))
    .filter(F.col("row_num") == 1)
)

latest = (
    ranked
    .withColumn("row_num_2", F.rank().over(w3))
    .filter(F.col("row_num_2") == 1)
)

scored = (
    latest
    .select(
        "*",
        F.min("last7_min_cost").over(w4).alias("order_id_min_last7_min_cost"),
        F.first("last7_min_cost", ignorenulls=True).over(w5).alias("first_known_last7_min_cost"),
    )
)

result = (
    scored
    .withColumn("last7_count_cost_tier", F.when(F.col("last7_count_cost") >= 18, "PREMIUM").otherwise("STANDARD"))
    .withColumn("activity_month", F.date_trunc("MONTH", "delivered_date"))
    .rollup("last7_count_cost_tier", "order_id", "activity_month")
    .agg(
        F.avg("last7_count_cost").alias("avg_last7_count_cost"),
        F.count(F.when(F.col("order_status") == "CANCELLED", 1)).alias("cancelled_rows"),
        F.count(F.when(F.col("order_status") == "COMPLETED", 1)).alias("completed_rows"),
        F.count(F.when(F.col("carrier") == "FedEx", 1)).alias("fedex_rows"),
        F.grouping("last7_count_cost_tier").alias("grouping_level"),
    )
    .filter(F.col("avg_last7_count_cost") > 0)
    .orderBy(F.col("order_id").asc_nulls_first())
)
