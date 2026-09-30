from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("created_at").desc(), "id")
w2 = Window.partitionBy("ship_country", "ship_city").orderBy(F.col("created_at").desc(), "id")
w3 = Window.partitionBy("customer_id").orderBy(F.col("created_at").desc(), "id")

parsed = (
    df_carts
    .select(
        "id",
        "customer_id",
        "created_at",
        "items",
        "ship_to",
        "attributes",
        F.col("ship_to.country").alias("ship_country"),
        F.col("attributes")["utm_source"].alias("utm_source"),
        F.col("ship_to.city").alias("ship_city"),
    )
)

result = (
    parsed
    .select(
        "*",
        F.row_number().over(w).alias("event_seq"),
        F.row_number().over(w2).alias("event_seq_2"),
        F.row_number().over(w3).alias("seq_no"),
        F.row_number().over(w3).alias("seq_no_2"),
        F.row_number().over(w2).alias("row_seq"),
    )
)
