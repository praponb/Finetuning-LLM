from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.length("store_name") < 13)
    .select("region")
    .distinct()
    .orderBy("region")
)
