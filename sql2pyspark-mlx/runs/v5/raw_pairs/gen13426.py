from pyspark.sql import functions as F

result = (
    df_stores
    .filter((F.col("store_name").like("%Rive%") & (F.col("region") == "East")) | F.col("opened_date").between("2024-05-01", "2025-03-14"))
    .select("id", "opened_date", "city", "sqft")
    .orderBy("id")
    .limit(1)
)
