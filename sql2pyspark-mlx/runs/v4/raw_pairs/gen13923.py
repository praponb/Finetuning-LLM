from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_running = Window.partitionBy("city").orderBy("signup_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("city")
w3 = Window.partitionBy("city", "segment").orderBy(F.col("credit_limit_share").desc_nulls_last())

metrics = (
    df_customers
    .select(
        "*",
        F.last("city", ignorenulls=True).over(w_running).alias("last_known_city"),
        F.round((F.col("credit_limit") / F.sum("credit_limit").over(w2)), 4).alias("credit_limit_share"),
    )
)

eligible = (
    metrics
    .alias("s")
    .join(df_orders.alias("c"), (F.col("c.customer_id") == F.col("s.id")) & (F.col("c.order_date").between("2025-01-01", "2025-09-30")), "left_anti")
)

not_flagged = (
    eligible
    .alias("s")
    .join(df_accounts.alias("ch"), (F.col("ch.customer_id") == F.col("s.id")) & (F.col("ch.opened_date") <= F.col("s.signup_date")), "left_anti")
)

ranked = (
    not_flagged
    .withColumn("rank_in_group", F.rank().over(w3))
    .filter(F.col("rank_in_group") == 1)
    .drop("rank_in_group")
)

result = (
    ranked
    .select(
        "id",
        "email",
        "country",
        "city",
        "segment",
        "signup_date",
        "credit_limit",
        "age",
        "last_known_city",
        F.when(F.col("country").rlike("^(UK|US)$"), "GROUP_A").otherwise("OTHER_ZONE").alias("country_zone"),
    )
    .orderBy("country_zone")
)
