from pyspark.sql import functions as F

result = (
    df_stores
    .filter(~(F.col("region").isin(["South", "North"]) | F.col("store_name").like("A%")))
    .select("region", "city", "opened_date")
    .orderBy("city", "region")
)
