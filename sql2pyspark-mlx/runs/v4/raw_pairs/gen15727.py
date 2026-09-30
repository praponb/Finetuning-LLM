from pyspark.sql import functions as F

result = (
    df_shipments
    .select("cost")
    .union(df_products.select("price"))
    .distinct()
)
