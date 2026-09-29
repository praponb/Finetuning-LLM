from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id", "device").orderBy(F.col("country_length").desc_nulls_last(), "id")
w_running = Window.orderBy("country_length", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("customer_id")
w4 = Window.partitionBy("country").orderBy(F.col("age").desc(), "id")

valid = (
    df_sessions
    .filter((F.col("started_at") >= F.lit("2023-04-11").cast("date")) & (F.col("device") == "mobile"))
    .select(
        "id",
        "customer_id",
        "device",
        "browser",
        "country",
        F.length("country").alias("country_length"),
    )
)

ranked = (
    valid
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
)

with_windows = (
    ranked
    .select(
        "*",
        F.first("country_length", ignorenulls=True).over(w_running).alias("first_known_country_length"),
        F.min("country_length").over(w3).alias("customer_id_min_country_length"),
    )
)

with_dims = (
    with_windows
    .alias("e")
    .join(df_customers.alias("b"), F.col("e.customer_id") == F.col("b.id"), "inner")
    .select("e.*", F.col("b.age"))
)

result = (
    with_dims
    .select(
        "*",
        F.ntile(5).over(w4).alias("age_bucket"),
        F.min("first_known_country_length").over(w3).alias("customer_id_min_first_known_country_length"),
    )
)
