from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("is_active"))
    .select("signup_date")
    .union(
        df_inventory
        .filter(F.col("product_id").isin([2, 7, 8]))
        .select("last_restock")
    )
    .orderBy("signup_date")
)
