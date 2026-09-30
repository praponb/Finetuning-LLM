from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.month("opened_date").isin([6, 7, 11]) & (F.col("opened_date") > "2023-03-30") & (F.upper(F.trim("store_name")) == "SIAM FLAGSHIP"))
    .orderBy("city")
)
