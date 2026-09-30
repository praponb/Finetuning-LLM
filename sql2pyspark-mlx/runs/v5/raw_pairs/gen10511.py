from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("plan", "status")

result = (
    df_subscriptions
    .select(
        "id",
        "plan",
        "status",
        "monthly_fee",
        F.round((F.col("monthly_fee") / F.sum("monthly_fee").over(w)), 3).alias("monthly_fee_share"),
    )
)
