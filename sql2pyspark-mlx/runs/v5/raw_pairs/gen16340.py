from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.upper(F.trim("store_name")) == "OLD TOWN")
    .select("opened_date")
    .union(df_store_sales.select("sale_date"))
    .orderBy(F.col("opened_date").desc())
)
