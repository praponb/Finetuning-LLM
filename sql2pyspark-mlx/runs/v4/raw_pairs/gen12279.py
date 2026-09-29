from pyspark.sql import functions as F

result = (
    df_shipments
    .filter((F.col("carrier") == "Flash") & F.col("delivered_date").between("2024-02-27", "2024-03-01"))
    .select("id", "cost", "order_id")
    .orderBy("id")
)
