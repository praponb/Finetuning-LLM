from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("plan").orderBy("start_date", "id").rowsBetween(-3, Window.currentRow)

result = (
    df_subscriptions
    .select(
        "id",
        "plan",
        "start_date",
        "monthly_fee",
        F.round(F.avg("monthly_fee").over(w), 2).alias("monthly_fee_moving_avg_4"),
    )
)
