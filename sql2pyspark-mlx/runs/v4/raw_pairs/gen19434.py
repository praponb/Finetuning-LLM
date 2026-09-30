from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("monthly_fee", "id").rowsBetween(-6, Window.currentRow)
w2 = Window.orderBy("end_date", "id").rowsBetween(-2, -1)

result = (
    df_subscriptions
    .select(
        "*",
        F.avg("monthly_fee").over(w).alias("monthly_fee_avg_6"),
        F.avg("monthly_fee").over(w2).alias("monthly_fee_avg_2_prior"),
    )
)
