from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id")
w2 = Window.partitionBy("plan", "status").orderBy("end_date", "id").rowsBetween(-6, -1)
w3 = Window.partitionBy("customer_id", "status").orderBy(F.col("end_date").desc(), "id")

result = (
    df_subscriptions
    .select(
        "*",
        F.round((F.col("monthly_fee") / F.sum("monthly_fee").over(w)), 4).alias("monthly_fee_share"),
        F.avg("monthly_fee").over(w2).alias("monthly_fee_avg_6_prior"),
        F.row_number().over(w3).alias("row_seq"),
    )
)
