from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.year("start_date") == 2024)
    .select("start_date")
    .union(df_stores.filter(F.col("city") == "Chiang Mai").select("opened_date"))
    .distinct()
)
