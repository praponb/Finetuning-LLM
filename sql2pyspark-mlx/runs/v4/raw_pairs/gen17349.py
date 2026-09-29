from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy("is_cancelled", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.orderBy("end_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("status").orderBy("end_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("status").orderBy(F.col("is_cancelled").desc(), "id")

valid = (
    df_subscriptions
    .filter((F.col("start_date") > F.lit("2024-03-10").cast("date")) & F.col("customer_id").isNotNull() & F.col("plan").isin(["enterprise", "basic", "premium"]))
    .select(
        "id",
        "customer_id",
        "start_date",
        "end_date",
        "status",
        F.when(F.col("status") == "CANCELLED", 1).otherwise(0).alias("is_cancelled"),
        F.when(F.col("monthly_fee") >= 269, "High").when(F.col("monthly_fee") >= 117, "Medium").otherwise("Low").alias("monthly_fee_level"),
    )
)

result = (
    valid
    .withColumn("running_is_cancelled", F.sum("is_cancelled").over(w))
    .withColumn("cumulative_is_cancelled", F.sum("is_cancelled").over(w2))
    .withColumn("last_known_is_cancelled", F.last("is_cancelled", ignorenulls=True).over(w3))
    .withColumn("is_cancelled_bucket", F.ntile(5).over(w4))
)
