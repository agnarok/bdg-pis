import redis
import json
import sys
import heapq
from operator import itemgetter

# Implementação do trabalho 4 de BD2
r = redis.Redis(decode_responses=True)

# Load data into redis -- Might take a while. Also needs about 1GB of free RAM
print('\nLoading data into redis....')
path = sys.argv[1]

def create_obj(array):
    obj = {}
    obj[array[0][0]] = int(array[0][1].strip())  # Id
    obj[array[1][0]] = array[1][1].strip()  # ASIN

    title = ''
    for til in array[2][1:-2]:
        title += til
    title += array[2][-1]
    obj[array[2][0].strip()] = title  # title

    obj[array[3][0].strip()] = array[3][1].strip()  # group
    obj[array[4][0].strip()] = int(array[4][1].strip())  # salesrank

    obj[array[5][0].strip()] = array[5][1][2:].split()  # similars

    n_categories = int(array[6][1].strip())
    obj[array[6][0].strip()] = []  # categories
    for i in range(0, n_categories):
        obj[array[6][0].strip()].append(array[i+7][0].strip())

    skip = 8+n_categories
    obj['reviews'] = []
    for i in range(skip, (len(array))):
        # print(array[skip+1][0].strip().split()[0])
        obj['reviews'].append({
            'date': array[i][0].strip().split()[0],
            'customer': array[i][1].strip().split()[0],
            'rating': int(array[i][1].strip().split()[2]),
            'votes': int(array[i][1].strip().split()[4]),
            'helpful': int(array[i][1].strip().split()[6])
        })

    return obj

# function to read a file with custom delimiter
def delimited(file, delimiter, bufsize=4096):
    buf = ''
    while True:
        newbuf = file.read(bufsize)
        if not newbuf:
            yield buf
            return
        buf += newbuf
        lines = buf.split(delimiter)
        for line in lines[:-1]:
            yield line
        buf = lines[-1]


def stringToArray(data):
  data = data.split('\n')
  if ('  discontinued product' in data or data[0].startswith('#')):
    return []
  return data

def formatArray(array):
  array = map(lambda obj: obj.split(':', 1), array)
  return list(array)

f = open(path, 'r')

delimited_file = delimited(f, delimiter='\n\n')
for data in delimited_file:
  # stop at last iteration
  if (len(data.replace('\n','')) == 0):
    break
  data = stringToArray(data)
  if len(data) > 0:
    data = formatArray(data)
    obj = create_obj(data)
    r.set(obj['ASIN'], json.dumps(obj))

print('\nCarregando dadso para o redis')


# encapsulate to avoid conflict with other exercises
def a():
  print('\nletra a)')
  # produto a procurar os similares com maior venda
  product = 'B00004R99S'

  def getProductByKey(key, redisInstance):
    raw_data = redisInstance.get(key)
    if (raw_data != None):
      return json.loads(raw_data)
    return None

  product_data = getProductByKey(product, r)
  product_reviews = product_data['reviews']
  product_reviews_max = sorted(product_reviews, key=itemgetter('helpful', 'rating'), reverse=True)
  product_reviews_min = sorted(
                        sorted(product_reviews, key=itemgetter('helpful'), reverse=True), key=itemgetter('rating'),
                        reverse=False)

  print('-------------------------\nPara o produto {}:'.format(product))
  print('-------------------------\n5 comentários mais úteis e com maior avaliação: ')
  for review in product_reviews_max[:5]:
    print(review)
  print('--------------------------\n5 comentários mais úteis com menor avaliação: ')
  for review in product_reviews_min[:5]:
    print(review)

a()


def b():
  # produto a procurar os similares com maior venda
  product = 'B00004R99S'
  print('\nletra b)')
  def getProductByKey(key, redisInstance):
    raw_data = redisInstance.get(key)
    if (raw_data != None):
      return json.loads(raw_data)
    return None

  def getSpecificFields(product, fields=['ASIN']):
    if (product != None):
      new_prd = {}
      for field in fields:
        new_prd[field] = product[field] 
      return new_prd
    return None

  product_data = getProductByKey(product, r)
  product_salesrank = product_data['salesrank']
  similars = product_data['similar']
  similars_salesrank = [getSpecificFields(getProductByKey(similar, r), ['ASIN', 'salesrank']) for similar in similars]
  similars_salesrank = filter(lambda x: x != None, similars_salesrank)

  print('\nProdutos similares a {} com mais vendas. (salesrank menor)'.format(product))
  for similar in similars_salesrank:
    if product_salesrank > similar['salesrank']:
      print(json.dumps(similar, indent=4))

b()

def c():
  print('\nletra c)')
  # produto a procurar os similares com maior venda
  product = '0486220125'

  def getProductByKey(key, redisInstance):
    raw_data = redisInstance.get(key)
    if (raw_data != None):
      return json.loads(raw_data)
    return None

  product_data = getProductByKey(product, r)
  product_reviews = sorted(product_data['reviews'], key=itemgetter('date'))

  ratings = []
  for index in range(0,len(product_reviews)):
      avg = 0
      for j in range(0,index+1):
          avg += product_reviews[j]['rating']
      avg = avg/(index+1)
      ratings.append((product_reviews[index]['date'],avg))

  # logic to print nicely
  print('\nEvolução diária das médias de avaliação de {}'.format(product))
  print('-'*35)
  for i in ratings:
    if(len(str(i)) < 33):
      i = str(i)
      i += ' ' * (33-len(i))
    print('|{}|'.format(i))
  print('-'*35)

c()

def d():
  print('\nLetra d)')
  def getProductByKey(key, redisInstance):
    raw_data = redisInstance.get(key)
    if (raw_data != None):
      return json.loads(raw_data)
    return None

  def getTopN(groups):
    for group in groups:
      print('\n10 produtos líderes de venda (menor salesrank) em {}:'.format(group))
      print('-----------------------------------------------------')
      topN = heapq.nsmallest(10, groupSet[group], key=itemgetter(1))
      for prd in topN:
        print('ASIN: {} com salesrank {}'.format(prd[0], prd[1]))
      print('-----------------------------------------------------')

  groupSet = {}
  cursor = '0'
  while cursor != 0:
    cursor, keys = r.scan(cursor=cursor, count=100)
    for key in keys:
      value = getProductByKey(key, r)
      if value['group'] not in groupSet and value['salesrank'] != -1:
        # initialize heap then push to it
        groupSet[value['group']] = []
        heapq.heappush(groupSet[value['group']], (value['ASIN'], value['salesrank']))
      elif(value['salesrank'] != -1):
        heapq.heappush(groupSet[value['group']], (value['ASIN'], value['salesrank']))

  getTopN(groupSet)

d()

def e():
  print('\nLetra e)')

  def getProductByKey(key, redisInstance):
    raw_data = redisInstance.get(key)
    if (raw_data != None):
      return json.loads(raw_data)
    return None

  def getTopN_E(values):
    print('\n10 produtos com a maior média de avaliações úteis positivas')
    print('-----------------------------------------------------')
    topN = heapq.nlargest(10, values, itemgetter(1))
    for prd in topN:
      print('ASIN: {} com média {}'.format(prd[0], prd[1]))
    print('-----------------------------------------------------')


  def calcAverage(reviews):
    avgRating = 0
    count = 1
    if len(reviews) > 0:
      for r in reviews:
        if(int(r['rating']) >= 5):
          count += 1
          avgRating += int(r['helpful'])
      return avgRating/count
    return 0.0

  groupSet = []
  cursor = '0'
  while cursor != 0:
    cursor, keys = r.scan(cursor=cursor, count=100)
    for key in keys:
      value = getProductByKey(key, r)
      heapq.heappush(groupSet, (value['ASIN'], calcAverage(value['reviews'])))

  getTopN_E(groupSet)

e()

def f():
  print('\nLetra f)')
  def getProductByKey(key, redisInstance):
    raw_data = redisInstance.get(key)
    if (raw_data != None):
      return json.loads(raw_data)
    return None

  def getTopN_F(groups):
    print('\n5 categorias com maior média de avaliações úteis positivas')
    print('-----------------------------------------------------')
    topN = heapq.nlargest(5, groupSet, itemgetter(1))
    for prd in topN:
      print('Category: {} com média: {}'.format(prd, groups[prd]))
    print('-----------------------------------------------------')

  def calcAverage(reviews):
    avgRating = 0
    count = 1
    if len(reviews) > 0:
      for r in reviews:
        if(int(r['rating']) >= 5):
          count += 1
          avgRating += int(r['helpful'])
      return avgRating/count
    return 0.0

  groupSet = {}
  cursor = '0'
  while cursor != 0:
    cursor, keys = r.scan(cursor=cursor, count=100)
    for key in keys:
      value = getProductByKey(key, r)
      categories = value['categories']
      for cat in categories:
        if cat not in groupSet and cat != None:
          groupSet[cat] = calcAverage(value['reviews'])
        elif(cat != None):
          groupSet[cat] = (calcAverage(value['reviews']) + groupSet[cat])/2

  getTopN_F(groupSet)

f()

def g():
  print('\nLetra g)')
  def getProductByKey(key, redisInstance):
    raw_data = redisInstance.get(key)
    if (raw_data != None):
      return json.loads(raw_data)
    return None

  def getSpecificFields(product, fields=['ASIN']):
    if (product != None):
      new_prd = {}
      for field in fields:
        new_prd[field] = product[field] 
      return new_prd
    return None

  def getTopN_G(groups):
    for group in groups:
      print('\n10 clientes com mais comentários em {}:'.format(group))
      print('-----------------------------------------------------')
      topN = heapq.nlargest(10, groupSet[group], key=groupSet[group].get)
      for prd in topN:
        print('Customer: {} com {} comentarios'.format(prd, groupSet[group][prd]))
      print('-----------------------------------------------------')

  groupSet = {}
  cursor = '0'
  while cursor != 0:
    cursor, keys = r.scan(cursor=cursor, count=100)
    for key in keys:
      value = getProductByKey(key, r)
      if value['group'] not in groupSet :
        groupSet[value['group']] = {}
      for review in value['reviews']:
        if review['customer'] not in groupSet[value['group']]:
          groupSet[value['group']][review['customer']] = 1
        else:
          groupSet[value['group']][review['customer']] += 1

  getTopN_G(groupSet)

g()