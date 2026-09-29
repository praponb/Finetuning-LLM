from pyspark.sql import functions as F

result = (
    df_stores
    .filter((F.col("city") == "Chiang Mai") & F.month("opened_date").isin([1, 3, 11]))
    .select("id", "opened_date", "city", "store_name")
)
