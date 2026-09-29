from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("amount").between(870, 1190) | (F.col("method") != "card"))
    .groupBy("status")
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("method"))).alias("method_values"),
    )
    .orderBy("method_values", "status")
)
