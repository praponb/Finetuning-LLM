from pyspark.sql import functions as F

result = (
    df_products
    .filter(~F.col("product_name").like("W%"))
    .select("id", "launch_date", "category", "cost")
    .orderBy(F.col("cost").desc())
)
