from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("amount") >= 1170)
    .cube(
        "channel",
        F.when(F.col("status").isin(["SHIPPED", "RETURNED"]), "core").otherwise("other").alias("status_group"),
    )
    .agg(F.sum("amount").alias("total_amount"))
)
