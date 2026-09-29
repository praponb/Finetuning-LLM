from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("segment").orderBy(F.col("age").desc())
w2 = Window.partitionBy("segment").orderBy("age", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_grp = Window.partitionBy("city").orderBy("rn", "id").rowsBetween(-5, -1)
w_recent = Window.orderBy("cumulative_age", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.orderBy("rn", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.partitionBy("segment", "country").orderBy("credit_limit_avg_5_prior", "id")
w7 = Window.partitionBy("city", "country").orderBy("signup_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w8 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.shipping_fee").desc(), F.col("c.id").desc())
w9 = Window.partitionBy("segment").orderBy("age", "id").rowsBetween(-6, Window.currentRow)
w10 = Window.partitionBy("city").orderBy(F.col("last_known_cumulative_age").desc(), "id")
w11 = Window.partitionBy("country")
w12 = Window.partitionBy("segment", "country")

latest = (
    df_customers
    .withColumn("rn", F.rank().over(w))
    .filter(F.col("rn") == 1)
)

with_windows = (
    latest
    .withColumn("cumulative_age", F.sum("age").over(w2))
    .withColumn("credit_limit_avg_5_prior", F.avg("credit_limit").over(w_grp))
)

with_windows_customers = (
    with_windows
    .select(
        "*",
        F.last("segment", ignorenulls=True).over(w_recent).alias("last_known_segment"),
        F.sum("cumulative_age").over(w5).alias("running_cumulative_age"),
        F.row_number().over(w6).alias("seq_no"),
        F.last("cumulative_age", ignorenulls=True).over(w7).alias("last_known_cumulative_age"),
    )
)

with_last = (
    with_windows_customers
    .alias("s")
    .join(df_orders.alias("c"), (F.col("c.customer_id") == F.col("s.id")), "left")
    .withColumn("rn_2", F.row_number().over(w8))
    .filter(F.col("rn_2") == 1)
    .select("s.*", F.col("c.discount").alias("order_discount"), F.col("c.amount"))
)

with_windows_customers_2 = (
    with_last
    .withColumn("order_discount_avg_6", F.avg("order_discount").over(w9))
    .withColumn("last_known_cumulative_age_bucket", F.ntile(10).over(w10))
    .withColumn("running_cumulative_age_share", F.round((F.col("running_cumulative_age") / F.sum("running_cumulative_age").over(w11)), 4))
    .withColumn("segment_sum_cumulative_age", F.sum("cumulative_age").over(w12))
)

result = (
    with_windows_customers_2
    .select(
        "id",
        "name",
        "country",
        "segment",
        "signup_date",
        "is_active",
        "credit_limit",
        "age",
        "rn",
    )
    .orderBy(F.col("id").desc_nulls_last())
)
