from pyspark.sql import functions as F

result = (
    df_payments
    .groupBy("method")
    .agg(
        F.count(F.when(F.col("method") == "card", 1)).alias("card_count"),
        F.sort_array(F.collect_set("method")).alias("method_list"),
    )
    .filter(F.col("card_count") > 2)
    .orderBy("method")
    .limit(10)
)
