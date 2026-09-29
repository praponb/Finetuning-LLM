from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("amount").between(700, 1090))
    .rollup("status", F.year("order_date").alias("order_year"))
    .agg(
        F.min("order_date").alias("first_order_date"),
        F.sum(F.when(F.col("channel") == "web", F.col("discount")).otherwise(0)).alias("web_discount"),
        F.sort_array(F.collect_set("status")).alias("status_list"),
    )
)
