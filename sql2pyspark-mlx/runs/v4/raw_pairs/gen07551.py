from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("method")
w2 = Window.partitionBy("order_id").orderBy("paid_at_day", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("method").orderBy(F.col("paid_at").cast("bigint")).rangeBetween(-604800, Window.currentRow)
w4 = Window.orderBy("paid_at", "id").rowsBetween(-3, Window.currentRow)

filtered = (
    df_payments
    .filter(F.col("order_id").isNotNull())
    .select(
        "*",
        F.quarter("paid_at").alias("paid_at_quarter"),
        F.col("paid_at").cast("date").alias("paid_at_day"),
        F.coalesce("method", F.lit("OTHER")).alias("method_filled"),
    )
)

scored = (
    filtered
    .select(
        "*",
        F.count("paid_at_quarter").over(w).alias("method_count_paid_at_quarter"),
        F.first("paid_at_quarter", ignorenulls=True).over(w2).alias("first_known_paid_at_quarter"),
        F.sum("paid_at_quarter").over(w3).alias("paid_at_quarter_sum_7d"),
        F.avg("paid_at_quarter").over(w4).alias("paid_at_quarter_avg_3"),
    )
)

joined = (
    scored
    .alias("src")
    .join(df_orders.alias("o"), F.col("src.order_id") == F.col("o.id"), "inner")
    .select("src.*", F.col("o.order_date"), F.col("o.status").alias("order_status"))
)

result = (
    joined
    .withColumn("week_start", F.date_trunc("WEEK", "order_date"))
    .withColumn("first_known_paid_at_quarter_tier", F.when(F.col("first_known_paid_at_quarter") >= 80, "LARGE").otherwise("SMALL"))
    .groupingSets(
        [["week_start", "first_known_paid_at_quarter_tier"], ["week_start"]],
        "week_start",
        "first_known_paid_at_quarter_tier",
    )
    .agg(
        F.sum(F.when(F.col("order_status") == "RETURNED", F.col("amount")).otherwise(0)).alias("returned_amount"),
        F.avg("method_count_paid_at_quarter").alias("avg_method_count_paid_at_quarter"),
        F.round(F.avg("paid_at_quarter_sum_7d"), 2).alias("avg_paid_at_quarter_sum_7d"),
    )
    .orderBy(F.col("avg_paid_at_quarter_sum_7d").desc_nulls_last())
)
