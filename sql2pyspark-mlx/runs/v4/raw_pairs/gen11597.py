from pyspark.sql import functions as F

result = (
    df_products
    .select("price")
    .subtract(
        df_orders
        .filter(F.month("order_date").isin([2, 5, 6]))
        .select("amount")
    )
    .orderBy("price")
)
