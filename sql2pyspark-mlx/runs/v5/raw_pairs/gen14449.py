from pyspark.sql import functions as F

result = (
    df_transactions
    .withColumn("is_flagged_label", F.when(F.col("is_flagged"), "Yes").otherwise("No"))
    .withColumn("amount_adjusted", F.round((F.col("amount") * 1.07), 2))
)
