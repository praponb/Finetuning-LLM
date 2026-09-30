from pyspark.sql import functions as F

result = (
    df_customers
    .select("signup_date")
    .unionByName(
        df_inventory
        .filter(F.col("quantity") < 142)
        .select(F.col("last_restock").alias("signup_date"))
    )
    .distinct()
    .orderBy("signup_date")
)
