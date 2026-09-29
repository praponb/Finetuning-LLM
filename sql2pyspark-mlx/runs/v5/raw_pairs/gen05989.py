from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.discount").desc(), F.col("c.id").desc())
w2 = Window.partitionBy("country")
w3 = Window.partitionBy("city").orderBy("signup_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_recent = Window.partitionBy("city").orderBy(F.col("credit_limit").desc(), "id")

valid = (
    df_customers
    .filter((F.col("signup_date") >= F.lit("2023-05-10").cast("date")) & (~F.col("city").eqNullSafe(F.lit("New York"))))
    .select(
        "*",
        F.coalesce("city", F.lit("Unknown")).alias("city_filled"),
        F.coalesce("city", F.lit("N/A")).alias("resolved_city"),
    )
)

with_last = (
    valid
    .alias("s")
    .join(df_orders.alias("c"), (F.col("c.customer_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.status"), F.col("c.channel").alias("order_channel"))
)

base = (
    with_last
    .filter((F.col("signup_date") >= F.lit("2023-03-09").cast("date")) & F.col("city").isNotNull())
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
        "city_filled",
        "resolved_city",
        "status",
        F.when(F.col("segment").isin(["Consumer", "Corporate"]), "Priority").otherwise("Other").alias("segment_group"),
        F.coalesce("segment", F.lit("N/A")).alias("resolved_segment"),
    )
)

with_windows = (
    base
    .select(
        "id",
        "country",
        "is_active",
        "credit_limit",
        "city_filled",
        "resolved_city",
        "segment_group",
        "resolved_segment",
        F.round((F.col("credit_limit") / F.sum("credit_limit").over(w2)), 4).alias("credit_limit_share"),
        F.first("credit_limit", ignorenulls=True).over(w3).alias("first_known_credit_limit"),
        F.ntile(5).over(w_recent).alias("credit_limit_bucket"),
    )
)

result = (
    with_windows
    .select(
        "id",
        "country",
        "city_filled",
        "segment_group",
        "resolved_segment",
        "credit_limit_share",
        "credit_limit_bucket",
        F.when(F.col("country").rlike("^(TH|SG)$"), "EXTENDED").otherwise("GROUP_A").alias("country_zone"),
    )
    .orderBy(F.col("credit_limit_share").desc_nulls_last())
)
