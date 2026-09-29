from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("balance") > 32000)
    .select("opened_date")
    .union(df_products.select("launch_date"))
    .distinct()
)
