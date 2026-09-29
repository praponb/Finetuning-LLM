from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("segment").orderBy("age")
w2 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.discount").desc(), F.col("c.id").desc())
w3 = Window.partitionBy("city", "order_status").orderBy(F.col("age").desc())
w4 = Window.partitionBy("city", "order_status").orderBy(F.col("credit_limit").asc_nulls_first(), "id")

base = (
    df_customers
    .filter(F.length("name") > 12)
    .select(
        "id",
        "name",
        "email",
        "country",
        "city",
        "segment",
        "signup_date",
        "is_active",
        "credit_limit",
        "age",
        F.when(F.col("is_active"), "Yes").otherwise("No").alias("is_active_label"),
        F.lower("segment").alias("segment_lower"),
    )
)

latest = (
    base
    .withColumn("dedup_rank", F.rank().over(w))
    .filter(F.col("dedup_rank") == 1)
)

with_last = (
    latest
    .alias("s")
    .join(df_orders.alias("c"), (F.col("c.customer_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w2))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.status").alias("order_status"))
)

eligible = (
    with_last
    .alias("s")
    .join(df_orders.alias("x"), (F.col("x.customer_id") == F.col("s.id")) & (F.col("x.order_date") < F.col("s.signup_date")), "left_semi")
)

ranked = (
    eligible
    .withColumn("row_num", F.rank().over(w3))
    .filter(F.col("row_num") == 1)
)

deduped = (
    ranked
    .withColumn("rn", F.row_number().over(w4))
    .filter(F.col("rn") == 1)
)

result = (
    deduped
    .select(
        "id",
        "name",
        "email",
        "segment",
        "is_active",
        "credit_limit",
        "age",
        "segment_lower",
        "dedup_rank",
        F.when(F.col("name").rlike("^(  Carol Diaz|David Lee )$"), "GROUP_B").otherwise("CORE").alias("name_zone"),
    )
    .orderBy("credit_limit")
)
