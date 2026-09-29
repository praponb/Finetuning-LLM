from pyspark.sql import functions as F

result = (
    df_orders
    .rollup(
        F.date_trunc("WEEK", "order_date").alias("week_start"),
        F.date_trunc("YEAR", "order_date").alias("year_start"),
    )
    .agg(
        F.count(F.when(F.col("status") == "COMPLETED", 1)).alias("completed_count"),
        F.size(F.collect_set("status")).alias("num_status"),
        F.count("*").alias("row_count"),
    )
)
