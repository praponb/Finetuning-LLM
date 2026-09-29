from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.balance").desc(), F.col("c.id").desc())

cleaned = (
    df_customers
    .filter((F.col("signup_date") > F.lit("2023-04-02").cast("date")) & F.col("segment").isNotNull())
    .select(
        "*",
        F.coalesce("name", F.lit("unassigned")).alias("name_filled"),
        F.coalesce("segment", F.lit("N/A")).alias("segment_clean"),
    )
)

result = (
    cleaned
    .alias("s")
    .join(df_accounts.alias("c"), (F.col("c.customer_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.opened_date"))
)
