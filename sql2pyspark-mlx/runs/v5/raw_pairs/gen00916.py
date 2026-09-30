from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.month("last_restock").isin([6, 8, 12]))
    .select("last_restock")
    .unionByName(
        df_customers
        .filter(F.col("country").isin(["DE", "SG"]))
        .select(F.col("signup_date").alias("last_restock"))
    )
    .distinct()
)
