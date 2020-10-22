import sys
orderDict = {}
basePath = sys.argv[1] #"/Users/zzh/Desktop/cockroach-data/extern/project-files/data-files/"
orderPath = basePath + "order.csv"
orders = open(orderPath).readlines()
for o in orders:
    o = o.split(',')
    o_w_id, o_d_id, o_id, o_c_id = o[0:4]
    orderDict.update({(o_w_id, o_d_id, o_id): o_c_id})

orderlinePath = basePath + "order-line.csv"
orderlineNewPath = basePath + "order-line-new.csv"
orderlines = open(orderlinePath).readlines()

orderlineNew = open(orderlineNewPath, 'w')
count = 0
for o in orderlines:
    ol_w_id, ol_d_id, ol_o_id = o.split(',')[0:3]
    ol_c_id = orderDict.get((ol_w_id, ol_d_id, ol_o_id))
    o = o[:-1] + ',' + ol_c_id + '\n'
    orderlineNew.write(o)
    if count < 5:
        print(o)
        count += 1
orderlineNew.close()


