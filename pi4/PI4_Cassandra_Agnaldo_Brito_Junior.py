from matplotlib.pyplot import figure
import numpy as np
import matplotlib.pyplot as plt
from cassandra.cluster import Cluster
from tqdm import tqdm


def parse(filename, total):
    IGNORE_FIELDS = ['Total items', 'reviews']
    f = open(filename, 'r')
    lines = f.readlines()
    entry = {}
    categories = []
    reviews = []
    similar_items = []

    for line in lines[0:total]:
        colonPos = line.find(':')

        if line.startswith("Id"):
            if reviews:
                entry["reviews"] = reviews
            if categories:
                entry["categories"] = categories
            yield entry
            entry = {}
            categories = []
            reviews = []
            rest = line[colonPos+2:]
            entry["id"] = rest[1:-1]

        elif line.startswith("similar"):
            similar_items = line.split()[2:]
            entry['similar_items'] = similar_items

    # "cutomer" is typo of "customer" in original data
        elif line.find("cutomer:") != -1:
            review_info = line.split()
            reviews.append({'customer_id': review_info[2],
                            'rating': int(review_info[4]),
                            'votes': int(review_info[6]),
                            'helpful': int(review_info[8]),
                            'date': review_info[0]})

        elif line.startswith("   |"):
            categories.append(line[0:-1].replace(' ', ''))

        elif colonPos != -1:
            eName = line[:colonPos]
            rest = line[colonPos+2:]
            if not eName in IGNORE_FIELDS:
                if eName[0] == ' ':
                    eName = eName[2:]
                entry[eName] = rest[0:-1].replace("'", "''")

    if reviews:
        entry["reviews"] = reviews
    if categories:
        entry["categories"] = categories

    yield entry


def read_file(file_path="data/amazon-meta.txt"):
    line_num = sum(1 for line in open(file_path))
    result = []
    for e in parse(file_path, total=line_num):
        if e:
            result.append(e)
    return result


# ### Código para a criação do esquema do banco de dados:


def create_schema(host, port, keyspace):
    cluster = Cluster([host], port=port)
    cursor = cluster.connect(keyspace)
    cursor.execute(f'''create table if not exists {keyspace}.products
    (
      id int,
      asin varchar,
      title varchar,
      group varchar,
      salesrank int,
      PRIMARY KEY (group,asin)
    );''')

    cursor.execute(f'''create table if not exists {keyspace}.similars
    (
      asin_1 varchar PRIMARY KEY,
      asin_2 varchar
    );
    ''')

    cursor.execute(f'''
    create table if not exists {keyspace}.categories
    (
      id int PRIMARY KEY,
      name varchar
    );
    ''')

    cursor.execute(f'''create table if not exists {keyspace}.product_category
    (
      product_asin varchar PRIMARY KEY,
      category_id int,
    );
    ''')

    cursor.execute(f'''create table if not exists {keyspace}.reviews
    (
      id int,
      date date,
      customer varchar,
      rating int,
      votes int,
      helpful int,
      product_asin varchar,
      PRIMARY KEY (customer,product_asin,id,rating,helpful)
    )
    ''')


def populate_database(host, port, keyspace, file_path):
    line_num = sum(1 for line in open(file_path))

    necessaryKeys = ["id", "ASIN", "title", "group",
                     "salesrank", "similar", "categories", "reviews"]

    cluster = Cluster([host], port=port)
    cursor = cluster.connect(keyspace)
    cursor.execute(f'TRUNCATE {keyspace}.reviews;')
    cursor.execute(f'TRUNCATE {keyspace}.product_category;')
    cursor.execute(f'TRUNCATE {keyspace}.categories;')
    cursor.execute(f'TRUNCATE {keyspace}.similars;')
    cursor.execute(f'TRUNCATE {keyspace}.products;')

    review_id = 0  # God forgive me

    # -------------------------------------
    for value in tqdm(parse(file_path, line_num), total=548552):
        if value:
            for key in necessaryKeys:
                if key not in list(value.keys()):
                    if key == 'group':
                        value[key] = "'Unkown'"
                    else:
                        value[key] = "null"
                else:
                    if key != 'id' and key != 'salesrank' and key != 'similar' and key != 'categories' and key != 'reviews':
                        value[key] = "'{}'".format(value[key])
            query = 'INSERT INTO {}.products (id,asin,title,group,salesrank) VALUES ({},{},{},{},{})'.format(keyspace, int(
                value['id']), value['ASIN'], value['title'], value['group'], value['salesrank'])
            cursor.execute(query)

            similars = value['similar'][1:].split('  ')[1:]
            if similars:
                for similar in similars:
                    query = 'INSERT INTO similars (asin_1,asin_2) VALUES '
                    query = query + \
                        '({},\'{}\')'.format(value['ASIN'], similar)
                    cursor.execute(query)
            if type(value['categories']) == list:
                for category in value['categories']:
                    category = category.split('|')
                    for x in category[1:]:
                        query = 'INSERT INTO categories (id,name) VALUES '
                        x = x.split('[')
                        try:
                            query = query + \
                                '({},\'{}\') IF NOT EXISTS'.format(
                                    int(x[1][:-1]), x[0].replace("'", "''"))
                        except:
                            query = query + \
                                '({},\'{}\') IF NOT EXISTS'.format(
                                    int(x[2][:-1]), x[0].replace("'", "''"))
                        cursor.execute(query)
                        try:
                            query = 'INSERT INTO product_category (product_asin,category_id) VALUES ({},{}) IF NOT EXISTS'.format(
                                value['ASIN'], int(x[1][:-1]))
                        except:
                            query = 'INSERT INTO product_category (product_asin,category_id) VALUES ({},{}) IF NOT EXISTS'.format(
                                value['ASIN'], int(x[2][:-1]))
                        cursor.execute(query)
            if type(value['reviews']) == list:
                for review in value['reviews']:
                    query = '''INSERT INTO 
                            reviews(id,date, customer, rating, votes, helpful, product_asin)
                            VALUES ({},\'{}\',\'{}\',{},{},{},{})'''.format(review_id, review['date'], review['customer_id'], review['rating'], review['votes'], review['helpful'], value['ASIN'])
                    cursor.execute(query)
                    review_id += 1

    # -------------------------------------


def getHelpfulReviews(asin, con):
    cursor = con
    return cursor.execute(f"SELECT * FROM teste.reviews WHERE product_asin='{asin}' ALLOW FILTERING ")


def getSimilarsWithMostSalesrank(asin, con):
    cursor = con
    similars = cursor.execute(
        f"SELECT asin_2 from similars WHERE asin_1='{asin}' ALLOW FILTERING")
    salesrank = []
    result = []
    for similar in similars:
        salesrank.append(cursor.execute(
            f"SELECT * from products where asin='{similar.asin_2}' ALLOW FILTERING"))
    for rank in salesrank:
        for r in rank:
            result.append(r)
    return result


def getProductAvgTimeline(asin, con):
    cursor = con
    rows = cursor.execute(
        f"SELECT date,rating FROM reviews WHERE product_asin='{asin}' ALLOW FILTERING")
    ratings = []
    rows = sorted(rows, key=lambda k: k.date, reverse=True)
    for index in range(0, len(rows)):
        avg = 0
        for j in range(0, index+1):
            avg += rows[j].rating
        avg = avg/(index+1)
        ratings.append((rows[index].rating, avg))
    return ratings


def getTop10ProductsOfEachGroup(con):
    cursor = con
    return cursor.execute(f"SELECT * FROM teste.products GROUP BY group")


def getTop10ProductsWithMostRatingAVG(con):
    cursor = con
    return cursor.execute(f"SELECT product_asin,avg(rating) FROM teste.reviews GROUP BY customer,product_asin LIMIT 10")


def getTop5CategoriesWithMostRating(con):
    cursor = con
    return cursor.execute(f"SELECT group,asin,salesrank FROM teste.products GROUP BY group LIMIT 5")


def get10CustomersWithMostReviewsForEachGroup(con):
    cursor = con
    return cursor.execute("SELECT customer,count(customer) FROM teste.reviews GROUP BY customer LIMIT 60 ALLOW FILTERING")


def create_connection(host, port, keyspace):
    cluster = Cluster([host], port=port)
    cursor = cluster.connect(keyspace)
    return cursor


if __name__ == "__main__":
    host = input('Host do Cassandra: ')
    port = int(input("Porta do Cassandra: "))
    keyspace = input('Nome do keyspace do Cassandra: ')
    path = input('Path para o arquivo de entrada (unziped): ')
    create_schema(host, port, keyspace)
    populate_database(host, port, keyspace, path)

    # a)
    print("\nResultados da query 5 - a)")
    con = create_connection(host, port, keyspace)
    result = getHelpfulReviews('0738700797', con)
    result = sorted(result, key=lambda k: k.rating, reverse=True)
    print("Comentários mais úteis e com maior avaliação e mais úteis com menos avaliação:")
    for product in result:
        print(
            f"| Customer: {product.customer} | Rating: {product.rating} | ASIN: {product.product_asin} |\n")
    input("Pressiona qualquer tecla para a próxima query")

    # b)
    print('\nResultados da query 5 - b)')

    con = create_connection(host, port, keyspace)
    result = getSimilarsWithMostSalesrank('0738700797', con)
    result = sorted(result, key=lambda k: k.salesrank, reverse=True)
    for row in result:
        print(f"{row}")
    input("Pressiona qualquer tecla para a próxima query")

    # c)
    print('\nResultados da query 5 - c)')
    con = create_connection(host, port, keyspace)
    result = getProductAvgTimeline('0738700797', con)
    print(result)
    input("Pressiona qualquer tecla para a próxima query")

    # d)
    print("\nResultados da query 5 - d)")
    con = create_connection(host, port, keyspace)
    result = getTop10ProductsOfEachGroup(con)
    for row in result:
        print(f'Group: {row.group}, Top Product: {row.title}')
    input("Pressiona qualquer tecla para a próxima query")

    # e)
    print("\nResultados da query 5 - e)")
    print("10 produtos com melhores reviews:")
    con = create_connection(host, port, keyspace)
    result = getTop10ProductsWithMostRatingAVG(con)
    for row in result:
        print(f"ASIN:{row.product_asin} , AVG_review: {row.system_avg_rating}")
    input("Pressiona qualquer tecla para a próxima query")

    # f)
    print("\nResultados da query 5 - f)")
    print("5 grupos com melhor avaliação")
    con = create_connection(host, port, keyspace)
    result = getTop5CategoriesWithMostRating(con)
    for row in result:
        print(f"Group:{row.group}")
    input("Pressiona qualquer tecla para a próxima query")

    # g)
    print("\nResultados da query 5 - g)")
    con = create_connection(host, port, keyspace)
    result = get10CustomersWithMostReviewsForEachGroup(con)
    for row in result:
        print(f"Customer: {row.customer}")
