from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("key", "customer_id").orderBy("created_at", "id", "key")
w_part = Window.orderBy("created_at", "id", "key").rowsBetween(Window.unboundedPreceding, Window.currentRow)

lines = (
    df_carts
    .select(
        "id",
        "customer_id",
        "created_at",
        F.explode("attributes").alias("key", "attr_value"),
    )
)

scored = (
    lines
    .filter(F.col("key").isin(["utm_source", "device", "gift"]))
    .select(
        "*",
        F.row_number().over(w).alias("seq_no"),
        F.first("key", ignorenulls=True).over(w_part).alias("first_known_key"),
    )
)

enriched = (
    scored
    .alias("src")
    .join(df_customers.alias("d"), F.col("src.customer_id") == F.col("d.id"), "inner")
    .select("src.*", F.col("d.email"), F.col("d.country"))
)

cleaned = (
    enriched
    .filter((F.col("created_at") >= F.lit("2025-03-01 17:00:00").cast("timestamp")) & F.hour("created_at").between(10, 18))
    .select(
        "*",
        F.length("email").alias("email_length"),
        F.coalesce("key", F.lit("DIRECT")).alias("key_filled"),
    )
)

result = (
    cleaned
    .groupBy("customer_id")
    .pivot("key", ["utm_source", "gift"])
    .agg(F.sum("seq_no").alias("amount"), F.count(F.lit(1)).alias("cnt"))
)
