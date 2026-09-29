from pyspark.sql import functions as F

result = (
    df_accounts
    .select("opened_date")
    .intersect(
        df_shipments
        .filter(F.col("order_id").isin([5, 13, 26, 33]))
        .select("delivered_date")
    )
)
