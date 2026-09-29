from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.col("city") == "Berlin")
    .select("opened_date")
    .unionByName(df_stores.filter(F.length("store_name") < 9).select("opened_date"))
)
