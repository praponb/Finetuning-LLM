from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id")
w_part = Window.orderBy("num_attributes", "id")
w_running = Window.partitionBy("customer_id").orderBy("num_attributes", "id")

unpacked = (
    df_carts
    .select(
        "id",
        "customer_id",
        "items",
        "attributes",
        F.coalesce(F.col("ship_to.country"), F.lit("UNKNOWN")).alias("country_or_unknown"),
        F.size("attributes").alias("num_attributes"),
    )
)

result = (
    unpacked
    .select(
        "*",
        F.count("num_attributes").over(w).alias("customer_id_count_num_attributes"),
        F.lead("num_attributes", 2).over(w_part).alias("next_num_attributes"),
        F.row_number().over(w_running).alias("row_seq"),
    )
)
