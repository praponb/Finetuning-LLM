from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_recent = Window.partitionBy("status").orderBy("monthly_fee", "id")
w2 = Window.partitionBy("customer_id")

metrics = (
    df_subscriptions
    .select(
        "id",
        "plan",
        "start_date",
        "end_date",
        "monthly_fee",
        "status",
        F.lag("monthly_fee").over(w_recent).alias("prev_monthly_fee"),
        F.row_number().over(w_recent).alias("row_seq"),
        F.count("monthly_fee").over(w2).alias("customer_id_count_monthly_fee"),
    )
)

result = (
    metrics
    .select(
        "id",
        "plan",
        "start_date",
        "monthly_fee",
        "status",
        "prev_monthly_fee",
        "row_seq",
        "customer_id_count_monthly_fee",
        F.when(F.col("plan").rlike("^(premium|standard)$"), "OTHER_ZONE").when(F.col("plan").rlike("^(basic|enterprise)$"), "GROUP_B").otherwise("CORE").alias("plan_zone"),
    )
)
