import random

podN = 5
taskN = 6

f = open('pod5_task6.txt', 'w')

for i in range(podN):
    for j in range(taskN):
        f.write(str(i) + ' ' + str(random.randint(0, 19)) + '\n')
