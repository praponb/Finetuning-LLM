from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("start_date", "id").rowsBetween(-6, Window.currentRow)

result = (
    df_subscriptions
    .select(
        "id",
        "start_date",
        "monthly_fee",
        F.round(F.avg("monthly_fee").over(w), 2).alias("monthly_fee_moving_avg_7"),
    )
)
