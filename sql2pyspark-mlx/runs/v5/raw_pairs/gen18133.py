from pyspark.sql import functions as F

max_discount = df_orders.filter(F.col("order_date").between("2023-08-01", "2023-12-01")).agg(F.max("discount")).first()[0]

result = (
    df_orders
    .filter(F.col("discount") >= max_discount)
    .select("id", "discount", "customer_id")
)
