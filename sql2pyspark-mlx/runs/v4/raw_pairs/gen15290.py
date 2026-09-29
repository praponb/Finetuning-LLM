from pyspark.sql import functions as F

result = (
    df_inventory
    .select("id", "quantity", "store_id")
    .orderBy("store_id", "id")
    .limit(50)
)
