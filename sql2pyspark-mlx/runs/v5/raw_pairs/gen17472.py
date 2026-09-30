from pyspark.sql import functions as F

result = (
    df_stores
    .filter(~F.col("store_name").like("R%") & (F.year("opened_date") >= 2023))
    .select("id", "sqft", "city", "region")
    .orderBy("city")
)
