from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("category") != "Sports")
    .select("id", "launch_date", "product_name", "brand")
)
