from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy("end_date", "id")
w2 = Window.partitionBy("plan").orderBy("end_date", "id")

result = (
    df_subscriptions
    .filter(F.col("end_date") <= "2025-05-01")
    .select(
        "id",
        "customer_id",
        "end_date",
        "monthly_fee",
        "plan",
        F.sum("monthly_fee").over(w).alias("running_monthly_fee"),
        F.lead("monthly_fee", 1).over(w2).alias("next_monthly_fee"),
    )
    .orderBy("end_date")
)
