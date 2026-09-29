from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("plan")
w2 = Window.partitionBy("status").orderBy("end_date", "id")

result = (
    df_subscriptions
    .select(
        "id",
        "plan",
        "monthly_fee",
        "status",
        "end_date",
        F.round((F.col("monthly_fee") / F.sum("monthly_fee").over(w)), 3).alias("monthly_fee_share"),
        F.first("plan").over(w2).alias("first_plan"),
    )
    .orderBy("status", F.col("monthly_fee").desc())
)
