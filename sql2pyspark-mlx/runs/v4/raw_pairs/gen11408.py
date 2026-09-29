from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("segment", "country").orderBy(F.col("age").desc())

eligible = (
    df_customers
    .alias("s")
    .join(df_orders.alias("ch"), (F.col("ch.customer_id") == F.col("s.id")) & (F.col("ch.status").isin(["SHIPPED", "CANCELLED", "PENDING"])), "left_anti")
)

ranked = (
    eligible
    .withColumn("row_num", F.rank().over(w))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

remaining_keys = (
    ranked
    .select("segment")
    .subtract(
        df_customers
        .filter(F.col("city") == "Tokyo")
        .select("segment")
    )
)

verified = (
    ranked
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.segment") == F.col("k.segment")), "inner")
    .select("s.*")
)

not_flagged = (
    verified
    .alias("s")
    .join(df_orders.alias("x"), (F.col("x.customer_id") == F.col("s.id")) & (F.col("x.store_id").isin([20, 33])), "left_anti")
)

result = (
    not_flagged
    .select(
        "id",
        "name",
        "email",
        "country",
        "city",
        "segment",
        "is_active",
        "credit_limit",
        "age",
    )
    .orderBy(F.col("age").desc())
)
