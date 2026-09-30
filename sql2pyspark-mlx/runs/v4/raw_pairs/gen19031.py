from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("brand").isin(["Acme", "Initech"]))
    .select("launch_date")
    .union(df_subscriptions.select("start_date"))
    .distinct()
    .orderBy("launch_date")
)
