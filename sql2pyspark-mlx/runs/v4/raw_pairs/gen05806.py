from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("product_name").like("%Make%"))
    .select("category", "launch_date")
    .unionByName(
        df_products
        .filter(F.col("brand").isin(["Umbrella", "Initech", "Stark"]))
        .select("category", "launch_date")
    )
)
