from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy("balance", "id")

ranked = (
    df_accounts
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
    .drop("rank_in_group")
)

result = (
    ranked
    .groupBy(
        "status",
        F.when(F.col("balance") >= 72000, "LARGE").otherwise("SMALL").alias("balance_tier"),
    )
    .agg(
        F.count(F.when(F.col("status") == "FROZEN", 1)).alias("frozen_rows"),
        F.count("*").alias("cnt"),
        F.sort_array(F.collect_set("status")).alias("status_list"),
    )
    .filter(F.col("frozen_rows") > 1)
    .orderBy(F.col("frozen_rows").desc_nulls_last())
)
