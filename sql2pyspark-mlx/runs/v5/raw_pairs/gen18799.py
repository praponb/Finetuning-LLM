from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id", "status").orderBy(F.col("amount").desc(), "id")
w2 = Window.partitionBy("status").orderBy("amount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("status")
w4 = Window.orderBy(F.col("order_date").desc(), "id")
w5 = Window.orderBy("amount")
w_part = Window.partitionBy("channel").orderBy("credit_limit", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

enriched = (
    df_orders
    .select(
        "*",
        F.row_number().over(w).alias("seq_no"),
        F.first("discount", ignorenulls=True).over(w2).alias("first_known_discount"),
        F.stddev_samp("discount").over(w3).alias("status_stddev_samp_discount"),
    )
)

with_windows = (
    enriched
    .withColumn("event_seq", F.row_number().over(w4))
    .withColumn("amount_rank", F.rank().over(w5))
)

with_dims = (
    with_windows
    .alias("src")
    .join(df_customers.alias("c"), F.col("src.customer_id") == F.col("c.id"), "left")
    .filter(F.col("c.signup_date") > "2023-03-23")
    .select("src.*", F.col("c.credit_limit"), F.col("c.age"))
)

with_windows_orders = (
    with_dims
    .select(
        "*",
        F.max("age").over(w_part).alias("running_max_age"),
        F.min("age").over(w_part).alias("running_min_age"),
        F.count("age").over(w_part).alias("running_count_age"),
    )
)

stats = (
    with_windows_orders
    .groupBy(
        F.when(F.col("first_known_discount") >= 0.37, "HIGH").otherwise("LOW").alias("first_known_discount_tier"),
        "status",
        F.when(F.col("discount") >= 0.32, "PREMIUM").otherwise("STANDARD").alias("discount_tier"),
    )
    .agg(
        F.count(F.when(F.col("channel") == "store", 1)).alias("store_count"),
        F.percentile_approx("running_min_age", 0.9).alias("running_min_age_p90"),
        F.max(F.when(F.col("channel") == "store", F.col("running_max_age"))).alias("store_max_running_max_age"),
        F.countDistinct("id").alias("distinct_ids"),
    )
)

result = (
    stats
    .select(
        "first_known_discount_tier",
        "status",
        "discount_tier",
        "store_count",
        "store_max_running_max_age",
        "distinct_ids",
        F.when(F.col("status").rlike("^(CANCELLED|SHIPPED)$"), "OTHER_ZONE").when(F.col("status").rlike("^(COMPLETED|PENDING)$"), "GROUP_B").otherwise("CORE").alias("status_zone"),
    )
    .orderBy(F.col("status").asc_nulls_last())
)
