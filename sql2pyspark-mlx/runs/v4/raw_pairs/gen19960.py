from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("status") == "SUCCESS")
    .groupBy(
        F.when(F.col("method").isin(["promptpay", "paypal"]), "core").otherwise("other").alias("method_group"),
        "method",
    )
    .agg(F.count("*").alias("cnt"))
    .filter(F.col("cnt") > 4)
    .orderBy(F.col("cnt").desc(), "method_group", "method")
    .limit(10)
)
