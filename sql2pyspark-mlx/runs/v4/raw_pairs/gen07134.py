from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("plan").orderBy("monthly_fee")
w2 = Window.partitionBy("status").orderBy("end_date", "id")

result = (
    df_subscriptions
    .select(
        "id",
        "plan",
        "monthly_fee",
        "status",
        "end_date",
        F.dense_rank().over(w).alias("monthly_fee_dense_rank"),
        F.lead("monthly_fee", 1).over(w2).alias("next_monthly_fee"),
    )
)
