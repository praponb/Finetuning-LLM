from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.upper(F.trim("store_name")) == "SIAM FLAGSHIP")
    .select("id", "region")
)
