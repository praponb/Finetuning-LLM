from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.month("launch_date").isin([1, 5, 7]))
    .select("launch_date", "product_name")
    .unionByName(
        df_products
        .filter(F.col("cost") >= 240)
        .select("launch_date", "product_name")
    )
    .distinct()
    .orderBy("launch_date")
)
