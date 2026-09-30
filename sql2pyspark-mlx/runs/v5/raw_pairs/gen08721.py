from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("plan").orderBy("end_date", "id").rowsBetween(-3, Window.currentRow)
w2 = Window.partitionBy("plan").orderBy(F.col("monthly_fee").desc())

result = (
    df_subscriptions
    .select(
        "id",
        "plan",
        "end_date",
        "monthly_fee",
        F.round(F.avg("monthly_fee").over(w), 2).alias("monthly_fee_moving_avg_4"),
        F.rank().over(w2).alias("rnk"),
    )
)
