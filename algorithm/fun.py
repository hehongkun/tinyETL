f = open('taskid.txt', 'r')
taskids = []
for line in f.readlines():
    a = int(line)
    taskids.append(a)
f.close()
f1 = open('pod10_task5.txt', 'r')
f2 = open('pod10_task5_ids.txt', 'w+')
for line in f1.readlines():
    a = line.split()[0]
    b = int(line.split()[1])
    f2.write(a + ' ' + str(taskids[b]) + '\n')
f2.close()
