from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy("start_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

enriched = (
    df_subscriptions
    .select(
        "id",
        "plan",
        "start_date",
        "end_date",
        "monthly_fee",
        "status",
        F.sum("monthly_fee").over(w).alias("running_sum_monthly_fee"),
        F.max("monthly_fee").over(w).alias("running_max_monthly_fee"),
        F.avg("monthly_fee").over(w).alias("running_avg_monthly_fee"),
    )
)

result = (
    enriched
    .select(
        "id",
        "plan",
        "start_date",
        "end_date",
        "monthly_fee",
        "running_max_monthly_fee",
        F.when(F.col("plan").rlike("^(premium|standard)$"), "GROUP_A").when(F.col("plan").rlike("^(basic|enterprise)$"), "CORE").otherwise("GROUP_B").alias("plan_zone"),
    )
)
