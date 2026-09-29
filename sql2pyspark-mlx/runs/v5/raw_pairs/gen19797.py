from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id")
w2 = Window.partitionBy("status").orderBy("end_date", "id")

base = (
    df_subscriptions
    .filter((F.col("status") == "CANCELLED") & F.col("customer_id").isNotNull())
    .select(
        "id",
        "customer_id",
        "start_date",
        "end_date",
        "monthly_fee",
        "status",
        F.concat_ws("/", "plan", "status").alias("plan_status_key"),
        F.date_format("end_date", "MMM yyyy").alias("end_date_label"),
    )
)

result = (
    base
    .filter(F.col("start_date") <= "2025-05-01")
    .select(
        "id",
        "customer_id",
        "start_date",
        "monthly_fee",
        "end_date_label",
        F.max("monthly_fee").over(w).alias("customer_id_max_monthly_fee"),
        F.lead("monthly_fee", 2).over(w2).alias("next_monthly_fee"),
    )
)
