from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("plan").orderBy("end_date", "id")
w2 = Window.partitionBy("customer_id").orderBy("end_date", "id")

result = (
    df_subscriptions
    .filter(F.col("status").isin(["PAUSED", "CANCELLED"]))
    .select(
        "id",
        "plan",
        "end_date",
        "monthly_fee",
        "customer_id",
        F.lead("monthly_fee", 1).over(w).alias("next_monthly_fee"),
        F.sum("monthly_fee").over(w2).alias("running_monthly_fee"),
    )
    .orderBy(F.col("end_date").desc())
)
