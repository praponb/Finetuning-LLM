from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("method_group")

result = (
    df_payments
    .filter(F.col("status") != "SUCCESS")
    .groupBy(
        F.when(F.col("method").isin(["promptpay", "paypal"]), "priority").otherwise("other").alias("method_group"),
    )
    .agg(F.sum("amount").alias("total_amount"))
    .withColumn("cumulative_amount", F.sum("total_amount").over(w))
    .orderBy(F.col("total_amount").desc(), "method_group")
    .limit(20)
)
