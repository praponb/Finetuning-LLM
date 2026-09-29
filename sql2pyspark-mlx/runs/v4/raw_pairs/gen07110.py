from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy(F.col("monthly_fee").desc_nulls_last())
w2 = Window.partitionBy("status").orderBy("monthly_fee", "id")

result = (
    df_subscriptions
    .select(
        "*",
        F.rank().over(w).alias("monthly_fee_rank"),
        F.ntile(5).over(w2).alias("monthly_fee_bucket"),
    )
)
