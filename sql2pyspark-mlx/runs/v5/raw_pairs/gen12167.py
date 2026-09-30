from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.array_contains("tags", "new") & F.col("category").isin(["Fashion", "Electronics", "Grocery"]))
    .select(
        "stock_qty",
        "cost",
        F.datediff(F.current_date(), "launch_date").alias("days_since_launch_date"),
    )
)
