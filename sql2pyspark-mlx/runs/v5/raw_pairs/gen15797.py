from pyspark.sql import functions as F

result = (
    df_stores
    .filter((F.col("opened_date") <= "2024-06-01") | (F.col("region") != "North"))
)
