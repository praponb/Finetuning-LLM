from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("quantity") > 104)
    .select("last_restock")
    .union(df_stores.filter(F.col("sqft") >= 9276).select("opened_date"))
    .orderBy("last_restock")
)
