from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy(F.col("end_date").desc())
w2 = Window.orderBy("is_premium", "id")
w3 = Window.orderBy("adjusted_monthly_fee", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

cleaned = (
    df_subscriptions
    .filter(F.col("plan") == "enterprise")
    .select(
        "id",
        "plan",
        "start_date",
        "end_date",
        "monthly_fee",
        "status",
        F.round((F.col("monthly_fee") * 100), 2).alias("adjusted_monthly_fee"),
    )
)

ranked = (
    cleaned
    .withColumn("rn", F.rank().over(w))
    .filter(F.col("rn") == 1)
    .select("id", "plan", "start_date", "monthly_fee", "adjusted_monthly_fee")
)

cleaned_subscriptions = (
    ranked
    .filter(F.col("start_date") > F.lit("2023-03-05").cast("date"))
    .select(
        "id",
        "start_date",
        "monthly_fee",
        "adjusted_monthly_fee",
        F.when(F.col("plan") == "premium", 1).otherwise(0).alias("is_premium"),
    )
)

metrics = (
    cleaned_subscriptions
    .select(
        "id",
        "monthly_fee",
        "adjusted_monthly_fee",
        "is_premium",
        F.row_number().over(w2).alias("seq_no"),
        F.last("adjusted_monthly_fee", ignorenulls=True).over(w3).alias("last_known_adjusted_monthly_fee"),
        F.ntile(5).over(w2).alias("is_premium_bucket"),
    )
)

result = (
    metrics
    .select(
        "id",
        "monthly_fee",
        "adjusted_monthly_fee",
        "is_premium",
        "last_known_adjusted_monthly_fee",
        "is_premium_bucket",
    )
    .orderBy("adjusted_monthly_fee")
)
