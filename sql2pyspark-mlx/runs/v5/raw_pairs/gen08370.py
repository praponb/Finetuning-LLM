from pyspark.sql import functions as F

result = (
    df_products
    .filter((F.col("brand") == "Umbrella") & (F.col("launch_date") >= "2023-07-14"))
)
