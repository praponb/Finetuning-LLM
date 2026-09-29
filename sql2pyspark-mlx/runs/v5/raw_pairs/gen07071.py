from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("account_id", "merchant").orderBy(F.col("amount").desc(), "id")

deduped = (
    df_transactions
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") == 1)
)

result = (
    deduped
    .select(
        "id",
        "txn_type",
        "amount",
        "is_flagged",
        F.when(F.col("txn_type").rlike("^(REFUND|CREDIT)$"), "GROUP_B").when(F.col("txn_type").rlike("^(DEBIT|FEE)$"), "OTHER_ZONE").otherwise("CORE").alias("txn_type_zone"),
    )
)
