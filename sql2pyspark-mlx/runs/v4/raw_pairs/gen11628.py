from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("plan") == "standard")
    .select("end_date")
    .union(
        df_store_sales
        .filter(F.col("product_id").isin([2, 4, 9]))
        .select("sale_date")
    )
    .distinct()
)
