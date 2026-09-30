from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("plan")
w2 = Window.orderBy("end_date")

result = (
    df_subscriptions
    .filter(F.col("plan") == "premium")
    .select(
        "id",
        "plan",
        "monthly_fee",
        "end_date",
        F.round((F.col("monthly_fee") / F.sum("monthly_fee").over(w)), 3).alias("monthly_fee_share"),
        F.rank().over(w2).alias("end_date_rank"),
    )
)
