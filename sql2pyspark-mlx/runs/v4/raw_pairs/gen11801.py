from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy(F.col("age").desc())

not_flagged = (
    df_customers
    .alias("s")
    .join(df_orders.alias("c"), (F.col("c.customer_id") == F.col("s.id")) & (F.col("c.order_date") >= F.col("s.signup_date")), "left_anti")
)

deduped = (
    not_flagged
    .withColumn("latest_rank", F.rank().over(w))
    .filter(F.col("latest_rank") == 1)
)

eligible = (
    deduped
    .alias("s")
    .join(df_accounts.alias("ch"), (F.col("ch.customer_id") == F.col("s.id")) & (F.col("ch.account_type").isin(["LOAN", "SAVINGS"])), "left_semi")
)

not_flagged_customers = (
    eligible
    .alias("s")
    .join(df_orders.alias("ch"), (F.col("ch.customer_id") == F.col("s.id")) & (F.col("ch.order_date") >= F.col("s.signup_date")), "left_anti")
)

result = (
    not_flagged_customers
    .select(
        "id",
        "name",
        "email",
        "country",
        "segment",
        "age",
        "latest_rank",
        F.when(F.col("name").rlike("^(Emma Brown|Henry Ford)$"), "CORE").otherwise("GROUP_A").alias("name_zone"),
    )
    .orderBy(F.col("latest_rank").desc_nulls_last())
)
