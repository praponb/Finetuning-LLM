from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("txn_type_group")

result = (
    df_transactions
    .groupBy(
        F.when(F.col("txn_type").isin(["REFUND", "DEBIT"]), "tier_1").otherwise("other").alias("txn_type_group"),
    )
    .agg(
        F.sum("amount").alias("total_amount"),
        F.count("*").alias("cnt"),
    )
    .withColumn("cumulative_amount", F.sum("total_amount").over(w))
    .orderBy("txn_type_group")
)
