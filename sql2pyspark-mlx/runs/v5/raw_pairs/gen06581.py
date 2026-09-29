from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("monthly_fee").isNull())
    .select("status", "plan")
    .subtract(
        df_subscriptions
        .filter(F.month("start_date").isin([6, 7, 12]))
        .select("status", "plan")
    )
)
