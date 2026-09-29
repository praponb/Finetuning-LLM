from pyspark.sql import functions as F

result = (
    df_stores
    .filter((F.col("opened_date") >= "2025-07-11") | F.col("store_name").like("Dow%"))
    .select("id", "city", "opened_date", "store_name")
)
