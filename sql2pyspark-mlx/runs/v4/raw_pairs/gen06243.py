from pyspark.sql import functions as F

result = (
    df_carts
    .select(
        "id",
        "customer_id",
        "items",
        F.col("attributes")["utm_source"].alias("utm_source"),
        F.size(F.filter("items", lambda li: li["qty"] >= 3)).alias("bulk_lines"),
    )
)
