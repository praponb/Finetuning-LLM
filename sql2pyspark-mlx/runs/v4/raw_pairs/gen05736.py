from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("txn_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("merchant").orderBy("amount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.orderBy("txn_time", "id")
w4 = Window.partitionBy("txn_type").orderBy("amount", "id")
w5 = Window.partitionBy("account_id", "merchant").orderBy("txn_time", "id").rowsBetween(-5, -1)

enriched = (
    df_transactions
    .withColumn("last_known_amount", F.last("amount", ignorenulls=True).over(w))
    .withColumn("first_known_merchant", F.first("merchant", ignorenulls=True).over(w2))
    .withColumn("seq_no", F.row_number().over(w3))
    .withColumn("next_amount", F.lead("amount", 1).over(w4))
    .withColumn("amount_avg_5_prior", F.avg("amount").over(w5))
)

result = (
    enriched
    .select(
        "id",
        "account_id",
        "txn_type",
        "amount",
        "txn_time",
        "merchant",
        "is_flagged",
        "last_known_amount",
        "first_known_merchant",
        F.when(F.col("txn_type").rlike("^(REFUND|DEBIT)$"), "CORE").when(F.col("txn_type").rlike("^(CREDIT|FEE)$"), "EXTENDED").otherwise("PRIORITY").alias("txn_type_zone"),
    )
    .orderBy(F.col("amount").desc_nulls_last())
)
