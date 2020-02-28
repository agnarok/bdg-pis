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
      asin varchar PRIMARY KEY,
      title varchar,
      group varchar,
      salesrank int
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
      id int PRIMARY KEY,
      date date,
      customer varchar,
      rating int,
      votes int,
      helpful int,
      product_asin varchar
    );
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


if __name__ == "__main__":
    host = "localhost"
    port = 9042
    keyspace = "teste"
    # data = read_file(file_path='data/amazon-meta.txt')
    create_schema(host, port, keyspace)
    populate_database(host, port, keyspace, 'data/amazon-meta.txt')
