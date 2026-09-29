from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("txn_type", "merchant").orderBy(F.col("amount").desc_nulls_last(), "id")

filtered = (
    df_transactions
    .filter(~F.col("merchant").eqNullSafe(F.lit("Lazada")))
    .withColumn("is_flagged_label", F.when(F.col("is_flagged"), "Yes").otherwise("No"))
    .withColumn("merchant_filled", F.coalesce("merchant", F.lit("N/A")))
)

result = (
    filtered
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") == 1)
    .select("id", "txn_type", "amount", "txn_time", "merchant", "is_flagged")
)
