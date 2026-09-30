from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(~F.col("merchant").isin(["Shell", "Grab"]))
    .groupBy("merchant", "is_flagged")
    .agg(F.round(F.avg("amount"), 2).alias("avg_amount"))
    .orderBy("merchant")
)
