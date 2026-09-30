from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("plan")
w2 = Window.partitionBy("status").orderBy("start_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

enriched = (
    df_subscriptions
    .select(
        "*",
        F.sum("monthly_fee").over(w).alias("plan_sum_monthly_fee"),
        F.first("monthly_fee", ignorenulls=True).over(w2).alias("first_known_monthly_fee"),
    )
)

result = (
    enriched
    .select(
        "id",
        "customer_id",
        "plan",
        "start_date",
        "end_date",
        "monthly_fee",
        "status",
        "first_known_monthly_fee",
        F.when(F.col("status").rlike("^(PAUSED|ACTIVE)$"), "OTHER_ZONE").when(F.col("status").rlike("^(CANCELLED|EXPIRED)$"), "CORE").otherwise("GROUP_A").alias("status_zone"),
    )
)
