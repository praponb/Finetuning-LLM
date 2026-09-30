from pyspark.sql import functions as F

rollup_totals = (
    df_orders
    .alias("e")
    .join(df_customers.alias("p"), F.col("e.customer_id") == F.col("p.id"), "inner")
    .groupingSets(
        [["country", "status"], ["country"], []],
        F.col("p.country").alias("country"),
        F.col("e.status").alias("status"),
    )
    .agg(
        F.count("*").alias("num_rows"),
        F.max(F.col("e.discount")).alias("max_discount"),
        F.max(F.col("e.amount")).alias("max_amount"),
    )
)

result = (
    rollup_totals
    .select("num_rows", "max_discount", "max_amount")
    .orderBy(F.col("num_rows").desc())
)
