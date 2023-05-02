import numpy as np
from pyMultiobjective.algorithm import non_dominated_sorting_genetic_algorithm_III
from sklearn import preprocessing
import math


taskfile = 'pod10_task5.txt'

taskResource = [[3.94, 1.3, 13.9], [11.6, 2.8, 7.5], [5.43, 7.8, 52.68], [3.28, 1.5, 28.44], [6.05, 1.1, 44.81], [6.92, 2.3, 31.71], [4.37, 0.7, 32.57], [5.25, 6.9, 23.13], [4.38, 3, 40.19], [
    8.03, 1, 66.58], [3.41, 1.8, 7.43], [2.86, 1.5, 15.91], [2.96, 3.7, 43.31], [5.35, 4.1, 55.9], [4.11, 2.3, 5.4], [6.86, 7.4, 53.9], [4.46, 2.1, 46.8], [2.92, 0.5, 3.35], [4.34, 0.2, 1.86], [4.14, 2.5, 31.13]]

nodeLoad = [[3.47, 15.9, 5.38], [2.48, 11.4, 5.12], [1.98, 7.23, 4.97], [2.02, 8.81, 4.94], [2.03, 7.09, 4.51], [
            1.92, 7.66, 4.55], [1.99, 8.23, 4.82], [1.93, 8.81, 4.82], [1.94, 7.96, 4.69], [2.31, 8.68, 4.59]]


class pod(object):
    def __init__(self, id):
        self.id = id
        self.cpu = 0
        self.mem = 0
        self.net = 0

    def addTask(self, taskid):
        self.cpu += taskResource[taskid][0]
        self.mem += taskResource[taskid][1]
        self.net += taskResource[taskid][2]


class NSGA_III:
    def __init__(self, pods):
        self.nodeLoad = [[3.47, 15.9, 5.38], [2.48, 11.4, 5.12], [1.98, 7.23, 4.97], [2.02, 8.81, 4.94], [2.03, 7.09, 4.51], [
            1.92, 7.66, 4.55], [1.99, 8.23, 4.82], [1.93, 8.81, 4.82], [1.94, 7.96, 4.69], [2.31, 8.68, 4.59]]
        self.pods = pods
        self.max_node = math.ceil(len(pods)/2)

    def run(self):
        min_values = []
        max_values = []
        for i in range(len(self.pods)):
            min_values.append(0)
            max_values.append(self.max_node - 1)

        parameters = {
            'references': 1,
            'min_values': min_values,
            'max_values': max_values,
            'mutation_rate': 0.1,
            'generations': 500,
            'mu': 1,
            'eta': 1,
            'k': 2,
            'verbose': False
        }
        sol = non_dominated_sorting_genetic_algorithm_III(
            list_of_functions=[self.funF, self.funG], **parameters)
        return sol

    def funF(self, variables=[0, 0]):
        nodeUsed = {}
        for i in range(len(self.pods)):
            target = (round(variables[i]))
            nodeUsed[target] = 1
        return len(nodeUsed.keys())

    def funG(self, variables=[0, 0]):
        load = [[0, 0, 0] for i in range(self.max_node)]
        nodeUsed = {}
        for i in range(len(self.pods)):
            target = abs(round(variables[i]))
            nodeUsed[target] = 0
            load[target][0] += pods[i].cpu
            load[target][1] += pods[i].mem
            load[target][2] += pods[i].net
        for k in nodeUsed.keys():
            load[k][0] += self.nodeLoad[k][0]
            load[k][1] += self.nodeLoad[k][1]
            load[k][2] += self.nodeLoad[k][2]
        cpunorm = np.asarray([load[k][0] for k in nodeUsed.keys()])
        memnorm = np.asarray([load[k][1] for k in nodeUsed.keys()])
        loadbalance = 0.0
        for i in range(len(cpunorm)):
            if cpunorm[i] >= 100 or memnorm[i] >= 100:
                loadbalance = 999
        return loadbalance


pods = []
f = open(taskfile, 'r')
for line in f.readlines():
    podid = int(line.split()[0])
    taskid = int(line.split()[1])
    if podid == len(pods):
        p = pod(podid)
        p.addTask(taskid)
        pods.append(p)
    else:
        pods[podid].addTask(taskid)
        
nsga = NSGA_III(pods)
sol_5 = nsga.run().tolist()
sol = []
for s in sol_5:
    if s[-1] == 0.0:
        sol = s
        break
for i in range(len(pods)):
    sol[i]=round(sol[i])
print(sol)

load = [[0, 0, 0] for i in range(math.ceil(len(pods)/2))]
nodeUsed = {}
pop = sol[0:len(pods)]
for i in range(len(pop)):
    if pop[i] not in nodeUsed.keys():
        nodeUsed[pop[i]] = [0,0,0]
    nodeUsed[pop[i]][0] += pods[i].cpu
    nodeUsed[pop[i]][1] += pods[i].mem
    nodeUsed[pop[i]][2] += pods[i].net


for k in nodeUsed.keys():
    nodeUsed[k][0] += nodeLoad[k][0]
    nodeUsed[k][1] += nodeLoad[k][1]
    nodeUsed[k][2] += nodeLoad[k][2]
cpuload = np.asarray([nodeUsed[k][0] for k in nodeUsed.keys()])
memload = np.asarray([nodeUsed[k][1] for k in nodeUsed.keys()])
netload = np.asarray([nodeUsed[k][2] for k in nodeUsed.keys()])

loadbalance = 0.0
for i in range(len(cpuload)):
    for j in range(i+1, len(cpuload)):
        loadbalance += abs(cpuload[i] - cpuload[j])
        loadbalance += abs(memload[i] - memload[j])
        loadbalance += abs(netload[i] - netload[j])
bestSol = {}
bestSol['sol'] = pop
bestSol['loadbalance'] = loadbalance
bestSol['nodeN'] = sol[-2]
print(bestSol)
print(cpuload)
print(memload)
print(netload)
