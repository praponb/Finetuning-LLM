from pyspark.sql import functions as F

result = (
    df_subscriptions
    .select("start_date")
    .subtract(df_stores.filter(F.col("city") == "London").select("opened_date"))
)
