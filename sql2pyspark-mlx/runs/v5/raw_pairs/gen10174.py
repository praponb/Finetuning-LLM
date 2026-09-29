from pyspark.sql import functions as F

result = (
    df_stores
    .filter((F.col("opened_date") <= "2023-08-28") & ~F.col("region").isin(["East", "Central", "South"]))
    .select("id", "city", "region")
)
