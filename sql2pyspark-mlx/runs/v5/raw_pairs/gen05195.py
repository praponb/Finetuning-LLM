from pyspark.sql import functions as F

result = (
    df_stores
    .select("id", "opened_date")
    .orderBy("id")
    .limit(3)
)
