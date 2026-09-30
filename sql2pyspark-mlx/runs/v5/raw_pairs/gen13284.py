from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.hour("txn_time").between(6, 13))
    .groupBy(
        F.when(F.col("merchant").isin(["Grab", "Lazada"]), "group_a").otherwise("other").alias("merchant_group"),
        "is_flagged",
    )
    .agg(
        F.sum(F.when(F.col("merchant") == "7-Eleven", 1).otherwise(0)).alias("7_eleven_count"),
        F.count(F.when(F.col("merchant") == "Grab", 1)).alias("grab_count"),
    )
    .filter(F.col("7_eleven_count") > 59)
    .select("merchant_group", "is_flagged", "7_eleven_count", "grab_count")
)
