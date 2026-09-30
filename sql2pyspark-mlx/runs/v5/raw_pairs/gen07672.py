from pyspark.sql import functions as F

result = (
    df_shipments
    .select("delivered_date")
    .subtract(df_products.select("launch_date"))
    .orderBy("delivered_date")
)
