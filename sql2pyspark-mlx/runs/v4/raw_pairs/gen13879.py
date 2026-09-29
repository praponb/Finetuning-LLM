from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id", "status").orderBy("monthly_fee_filled", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.orderBy("start_date", "id").rowsBetween(-2, -1)
w3 = Window.partitionBy("status").orderBy("last_known_monthly_fee", "id")
w4 = Window.orderBy("monthly_fee_filled", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.orderBy("last_known_monthly_fee", "id")
w6 = Window.partitionBy("status").orderBy("monthly_fee", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w7 = Window.orderBy(F.col("monthly_fee_filled").desc())

filtered = (
    df_subscriptions
    .filter((F.col("start_date") > F.lit("2023-11-29").cast("date")) & F.col("customer_id").isNotNull())
    .select(
        "id",
        "customer_id",
        "start_date",
        "monthly_fee",
        "status",
        F.coalesce("monthly_fee", F.lit(0)).alias("monthly_fee_filled"),
    )
)

with_windows = (
    filtered
    .withColumn("last_known_monthly_fee", F.last("monthly_fee", ignorenulls=True).over(w))
    .withColumn("monthly_fee_filled_avg_2_prior", F.avg("monthly_fee_filled").over(w2))
)

scored = (
    with_windows
    .select(
        "*",
        F.ntile(10).over(w3).alias("last_known_monthly_fee_bucket"),
        F.first("monthly_fee", ignorenulls=True).over(w4).alias("first_known_monthly_fee"),
        F.row_number().over(w5).alias("row_seq"),
        F.sum("last_known_monthly_fee").over(w6).alias("running_last_known_monthly_fee"),
        F.rank().over(w7).alias("monthly_fee_filled_rank"),
    )
)

result = (
    scored
    .select(
        "id",
        "start_date",
        "monthly_fee_filled",
        "last_known_monthly_fee",
        "monthly_fee_filled_avg_2_prior",
        "first_known_monthly_fee",
        "row_seq",
    )
    .orderBy("id")
)
