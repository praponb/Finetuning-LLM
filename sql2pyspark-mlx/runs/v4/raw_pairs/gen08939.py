from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy("monthly_fee", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("status").orderBy("monthly_fee", "id").rowsBetween(-6, -1)
w3 = Window.partitionBy("customer_id")

scored = (
    df_subscriptions
    .select(
        "*",
        F.sum("monthly_fee").over(w).alias("running_monthly_fee"),
        F.avg("monthly_fee").over(w2).alias("monthly_fee_avg_6_prior"),
        F.max("monthly_fee").over(w3).alias("customer_id_max_monthly_fee"),
    )
)

result = (
    scored
    .select(
        "id",
        "monthly_fee",
        "status",
        "running_monthly_fee",
        "monthly_fee_avg_6_prior",
        F.when(F.col("status").rlike("^(EXPIRED|ACTIVE)$"), "CORE").when(F.col("status").rlike("^(CANCELLED|PAUSED)$"), "GROUP_B").otherwise("GROUP_A").alias("status_zone"),
    )
    .orderBy(F.col("id").desc_nulls_last())
    .limit(10)
)
