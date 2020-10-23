import sys
basePath = sys.argv[1]
experimentNum = int(sys.argv[2])
head = []
resultLines = []

def getClientNumber(line):
    return int(line.split(',')[1])


def getThroughput(line):
    return float(line.split(',')[4])


for i in range(1, 6):
    result = open(basePath + "/clients-" + str(experimentNum) + "-" + str(i) + ".csv").readlines()
    if i == 1:
        head.append(result[0])
    resultLines += result[1:]
resultLines.sort(key=getClientNumber)
newFile = open(basePath + "/clients-" + str(experimentNum) + ".csv", 'w')
newFile.writelines(head)
newFile.writelines(resultLines)
newFile.close()

throughput = []
for l in resultLines[1:]:
    throughput.append(getThroughput(l))
print("Experiment " + str(experimentNum) + ":")
print("Min throughput: " + str(min(throughput)))
print("Max throughput: " + str(max(throughput)))
print("Mean throughput: " + str(sum(throughput) / len(throughput)))


