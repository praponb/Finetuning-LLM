from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("txn_type").orderBy(F.col("amount").asc_nulls_first(), "id")

latest = (
    df_transactions
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") == 1)
)

result = (
    latest
    .select(
        "id",
        "txn_time",
        "merchant",
        "is_flagged",
        "row_num",
        F.when(F.col("merchant").rlike("^(Grab|Amazon)$"), "EXTENDED").otherwise("OTHER_ZONE").alias("merchant_zone"),
    )
)
