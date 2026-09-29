from pyspark.sql import functions as F

eligible = (
    df_customers
    .alias("s")
    .join(df_sessions.alias("ch"), (F.col("ch.customer_id") == F.col("s.id")) & (F.col("ch.started_at") >= F.col("s.signup_date")), "left_anti")
)

result = (
    eligible
    .select(
        "id",
        "name",
        "country",
        "segment",
        "age",
        F.when(F.col("name").rlike("^(Alice Wong|Grace Kim)$"), "PRIORITY").when(F.col("name").rlike("^(bob smith|  Carol Diaz)$"), "EXTENDED").otherwise("GROUP_B").alias("name_zone"),
    )
    .orderBy(F.col("id").desc_nulls_first())
)
