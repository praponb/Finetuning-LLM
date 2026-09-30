from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy("end_date", "id")

result = (
    df_subscriptions
    .filter(F.col("monthly_fee").between(70, 80))
    .select(
        "id",
        "status",
        "end_date",
        "monthly_fee",
        F.lead("monthly_fee", 1).over(w).alias("next_monthly_fee"),
    )
    .orderBy("status")
)
