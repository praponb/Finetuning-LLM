from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.upper(F.trim("product_name")) == "OFFICE CHAIR")
    .agg((F.max("cost") - F.min("cost")).alias("cost_range"))
)
