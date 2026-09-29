from pyspark.sql import functions as F

result = (
    df_products
    .filter((F.col("launch_date") <= "2023-04-01") & ((F.col("brand") != "Initech") | F.col("supplier_id").isin([8, 28, 31])))
    .select("id", "supplier_id", "brand")
    .orderBy("brand")
)
