from pyspark.sql import functions as F

result = (
    df_transactions
    .withColumn("adjusted_amount", F.round((F.col("amount") * 1.07), 2))
    .withColumn("is_flagged_label", F.when(F.col("is_flagged"), "Yes").otherwise("No"))
)
