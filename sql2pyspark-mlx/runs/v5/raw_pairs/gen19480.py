from pyspark.sql import functions as F

result = (
    df_employees
    .select(F.col("hire_date").alias("event_date"))
    .intersect(
        df_inventory
        .filter(F.col("product_id").isin([2, 3, 10]))
        .select(F.col("last_restock").alias("event_date"))
    )
)
