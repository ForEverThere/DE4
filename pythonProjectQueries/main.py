from pyspark.sql import SparkSession
from pyspark.sql.functions import desc
from pyspark.sql.functions import col
from pyspark.sql import functions as func

spark = SparkSession \
    .builder \
    .appName("Python") \
    .config("spark.jars", "/home/vlad/postgresql-42.2.6.jar") \
    .getOrCreate()

film_category = spark.read \
    .format("jdbc") \
    .option("url", "jdbc:postgresql://localhost:5432/vladde") \
    .option("dbtable", "film_category") \
    .option("user", "vlad") \
    .option("password", "167943") \
    .option("driver", "org.postgresql.Driver") \
    .load()

category = spark.read \
    .format("jdbc") \
    .option("url", "jdbc:postgresql://localhost:5432/vladde") \
    .option("dbtable", "category") \
    .option("user", "vlad") \
    .option("password", "167943") \
    .option("driver", "org.postgresql.Driver") \
    .load()

film = spark.read \
    .format("jdbc") \
    .option("url", "jdbc:postgresql://localhost:5432/vladde") \
    .option("dbtable", "film") \
    .option("user", "vlad") \
    .option("password", "167943") \
    .option("driver", "org.postgresql.Driver") \
    .load()

film_actor = spark.read \
    .format("jdbc") \
    .option("url", "jdbc:postgresql://localhost:5432/vladde") \
    .option("dbtable", "film_actor") \
    .option("user", "vlad") \
    .option("password", "167943") \
    .option("driver", "org.postgresql.Driver") \
    .load()

actor = spark.read \
    .format("jdbc") \
    .option("url", "jdbc:postgresql://localhost:5432/vladde") \
    .option("dbtable", "actor") \
    .option("user", "vlad") \
    .option("password", "167943") \
    .option("driver", "org.postgresql.Driver") \
    .load()

inventory = spark.read \
    .format("jdbc") \
    .option("url", "jdbc:postgresql://localhost:5432/vladde") \
    .option("dbtable", "inventory") \
    .option("user", "vlad") \
    .option("password", "167943") \
    .option("driver", "org.postgresql.Driver") \
    .load()

rental = spark.read \
    .format("jdbc") \
    .option("url", "jdbc:postgresql://localhost:5432/vladde") \
    .option("dbtable", "rental") \
    .option("user", "vlad") \
    .option("password", "167943") \
    .option("driver", "org.postgresql.Driver") \
    .load()

payment = spark.read \
    .format("jdbc") \
    .option("url", "jdbc:postgresql://localhost:5432/vladde") \
    .option("dbtable", "payment") \
    .option("user", "vlad") \
    .option("password", "167943") \
    .option("driver", "org.postgresql.Driver") \
    .load()

city = spark.read \
    .format("jdbc") \
    .option("url", "jdbc:postgresql://localhost:5432/vladde") \
    .option("dbtable", "city") \
    .option("user", "vlad") \
    .option("password", "167943") \
    .option("driver", "org.postgresql.Driver") \
    .load()

address = spark.read \
    .format("jdbc") \
    .option("url", "jdbc:postgresql://localhost:5432/vladde") \
    .option("dbtable", "address") \
    .option("user", "vlad") \
    .option("password", "167943") \
    .option("driver", "org.postgresql.Driver") \
    .load()

customer = spark.read \
    .format("jdbc") \
    .option("url", "jdbc:postgresql://localhost:5432/vladde") \
    .option("dbtable", "customer") \
    .option("user", "vlad") \
    .option("password", "167943") \
    .option("driver", "org.postgresql.Driver") \
    .load()
# 7
task = input('Input number of task(1-7): ')

if int(task) == 1:
    category \
        .join(film_category, category['category_id'] == film_category['category_id'], 'inner') \
        .groupBy(category['name']) \
        .count() \
        .orderBy(desc('count')) \
        .show()

elif int(task) == 2:
    actor \
        .join(film_actor, actor['actor_id'] == film_actor['actor_id'], 'inner') \
        .join(film, film['film_id'] == film_actor['film_id'], 'inner') \
        .join(inventory, inventory['film_id'] == film['film_id'], 'inner') \
        .join(rental, rental['inventory_id'] == inventory['inventory_id'], 'inner') \
        .groupBy(rental['inventory_id'], inventory['film_id'], actor['first_name'], actor['last_name']).count() \
        .orderBy(desc('count')).limit(10).show()

elif int(task) == 3:
    result = category \
        .join(film_category, film_category['category_id'] == category['category_id'], 'inner') \
        .join(film, film['film_id'] == film_category['film_id'], 'inner') \
        .join(inventory, inventory['film_id'] == film['film_id'], 'inner') \
        .join(rental, rental['inventory_id'] == inventory['inventory_id'], 'inner') \
        .join(payment, payment['rental_id'] == rental['rental_id'], 'inner')
    result[['name', 'amount']].groupBy(category['name']).sum('amount').orderBy(desc('sum(amount)')).limit(1).show()

elif int(task) == 4:
    res = film.join(inventory, how='left_anti', on=['film_id'])
    res[['title']].show()

elif int(task) == 5:
    category \
        .join(film_category, category['category_id'] == film_category['category_id'], 'inner') \
        .join(film, film['film_id'] == film_category['film_id'], 'inner') \
        .join(film_actor, film['film_id'] == film_actor['film_id'], 'inner') \
        .join(actor, film_actor['actor_id'] == actor['actor_id']) \
        .filter(category['name'] == 'Children').groupBy(actor['first_name'], actor['last_name']) \
        .count().orderBy(desc('count')).limit(3).show()

elif int(task) == 6:
    city \
        .join(address, city['city_id'] == address['city_id'], 'inner') \
        .join(customer, address['address_id'] == customer['address_id'], 'inner') \
        .groupBy(city['city']).agg({'active': 'count'}).sort(col('count(active)').desc()).show()

elif int(task) == 7:
    city \
        .join(address, city['city_id'] == address['city_id'], 'inner') \
        .join(customer, address['address_id'] == customer['address_id'], 'inner') \
        .join(payment, customer['customer_id'] == payment['customer_id'], "inner") \
        .join(rental, payment['rental_id'] == rental['rental_id'], "inner") \
        .join(inventory, rental['inventory_id'] == inventory['inventory_id'], "inner") \
        .join(film_category, inventory['film_id'] == film_category['film_id'], "inner") \
        .join(category, film_category['category_id'] == category['category_id'], "inner") \
        .filter((col('name').like("%-%")) | (col('name').like("A%"))).groupBy(category["name"]) \
        .agg(func.max(rental['rental_id'])).sort(col('max(rental_id)').desc()).limit(1).show()
else:
    print('Error, task ' + str(task) + " doesn't exists")
