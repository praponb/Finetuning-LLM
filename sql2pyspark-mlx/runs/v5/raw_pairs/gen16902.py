from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("txn_type").orderBy(F.col("txn_type_length").desc())

cleaned = (
    df_transactions
    .filter(F.col("txn_time") >= "2024-03-01")
    .select(
        "*",
        F.when(F.col("merchant") == "Grab", 1).otherwise(0).alias("is_grab"),
        F.length("txn_type").alias("txn_type_length"),
    )
)

result = (
    cleaned
    .withColumn("rank_in_group", F.rank().over(w))
    .filter(F.col("rank_in_group") == 1)
)
