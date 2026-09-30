from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy("end_date", "id").rowsBetween(-2, Window.currentRow)

result = (
    df_subscriptions
    .filter(F.col("end_date") > "2023-04-14")
    .select(
        "id",
        "status",
        "end_date",
        "monthly_fee",
        F.round(F.avg("monthly_fee").over(w), 2).alias("monthly_fee_moving_avg_3"),
    )
)
