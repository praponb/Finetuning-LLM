from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_part = Window.partitionBy("plan").orderBy(F.col("monthly_fee").desc(), "id")
w2 = Window.partitionBy("status")
w3 = Window.orderBy("start_date", "id")

result = (
    df_subscriptions
    .select(
        "*",
        F.ntile(3).over(w_part).alias("monthly_fee_bucket"),
        F.round((F.col("monthly_fee") / F.sum("monthly_fee").over(w2)), 4).alias("monthly_fee_share"),
        F.row_number().over(w3).alias("row_seq"),
    )
)
