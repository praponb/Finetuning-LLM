from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.hour("txn_time").between(11, 16))
    .withColumn("is_flagged_label", F.when(F.col("is_flagged"), "Yes").otherwise("No"))
)
