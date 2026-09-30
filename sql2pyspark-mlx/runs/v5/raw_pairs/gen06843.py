from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.col("city") != "New York")
    .orderBy("opened_date")
)
