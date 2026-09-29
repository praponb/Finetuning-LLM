from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy(F.col("opened_date_year").asc_nulls_first())
w2 = Window.partitionBy("status", "country").orderBy("balance", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

not_flagged = (
    df_accounts
    .alias("s")
    .join(df_transactions.alias("x"), (F.col("x.account_id") == F.col("s.id")) & (F.col("x.merchant") != "Amazon"), "left_semi")
)

base = (
    not_flagged
    .filter(F.col("opened_date") > F.lit("2023-04-01").cast("date"))
    .withColumn("opened_date_year", F.year("opened_date"))
    .withColumn("status_clean", F.coalesce("status", F.lit("N/A")))
)

joined = (
    base
    .alias("s")
    .join(df_customers.alias("d"), F.col("s.customer_id") == F.col("d.id"), "inner")
    .select("s.*", F.col("d.country"))
)

result = (
    joined
    .select(
        "*",
        F.rank().over(w).alias("opened_date_year_rank"),
        F.last("opened_date_year", ignorenulls=True).over(w2).alias("last_known_opened_date_year"),
    )
)
