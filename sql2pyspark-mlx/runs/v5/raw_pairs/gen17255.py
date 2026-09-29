from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.year("last_restock") >= 2023)
    .select("last_restock")
    .intersect(df_stores.select("opened_date"))
)
