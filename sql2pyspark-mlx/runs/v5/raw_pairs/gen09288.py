from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("txn_type").orderBy(F.col("amount").asc_nulls_last(), "id")

ranked = (
    df_transactions
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
)

result = (
    ranked
    .select(
        "id",
        "txn_type",
        "amount",
        "merchant",
        "rank_in_group",
        F.when(F.col("merchant").rlike("^(Lazada|7-Eleven)$"), "EXTENDED").when(F.col("merchant").rlike("^(Grab|Amazon)$"), "GROUP_B").otherwise("OTHER_ZONE").alias("merchant_zone"),
    )
    .orderBy(F.col("merchant").desc_nulls_first())
)
