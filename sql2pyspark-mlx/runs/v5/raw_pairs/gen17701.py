from pyspark.sql import functions as F

result = (
    df_stores
    .filter(~F.col("city").isin(["New York", "Tokyo"]) & (F.upper(F.trim("store_name")) == "SIAM FLAGSHIP"))
    .select("store_name", "opened_date")
    .orderBy("store_name")
)
