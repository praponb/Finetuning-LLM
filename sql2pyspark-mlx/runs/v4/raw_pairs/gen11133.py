from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("start_date", "id")
w2 = Window.partitionBy("plan").orderBy("end_date", "id")

result = (
    df_subscriptions
    .filter(F.col("monthly_fee") >= 200)
    .select(
        "id",
        "start_date",
        "monthly_fee",
        "plan",
        "end_date",
        F.lead("monthly_fee", 1).over(w).alias("next_monthly_fee"),
        F.lead("monthly_fee", 1).over(w2).alias("next_monthly_fee_2"),
    )
)
