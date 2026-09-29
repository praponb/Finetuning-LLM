from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("supplier_id") == 24)
    .select("price")
    .union(
        df_payments
        .filter(F.to_date("paid_at") >= "2024-06-01")
        .select("amount")
    )
    .distinct()
    .orderBy("price")
)
