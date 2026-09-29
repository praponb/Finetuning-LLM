from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy(F.col("monthly_fee").desc())
w2 = Window.partitionBy("plan")

result = (
    df_subscriptions
    .select(
        "id",
        "status",
        "monthly_fee",
        "plan",
        F.dense_rank().over(w).alias("monthly_fee_dense_rank"),
        F.round((F.col("monthly_fee") / F.sum("monthly_fee").over(w2)), 3).alias("monthly_fee_share"),
    )
)
