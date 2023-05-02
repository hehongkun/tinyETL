import random
import numpy as np
import math
from sklearn import preprocessing


podPerNode = 2
popN = 50
Ngeneratios = 50
PC = 0.6
PM = 1
eta_m = 1
eta_c = 1
taskfile = 'pod10_task5.txt'

random.seed(10)
np.random.seed(10)

nodeLoad = [[3.47, 15.9, 5.38], [2.48, 11.4, 5.12], [1.98, 7.23, 4.97], [2.02, 8.81, 4.94], [2.03, 7.09, 4.51], [
    1.92, 7.66, 4.55], [1.99, 8.23, 4.82], [1.93, 8.81, 4.82], [1.94, 7.96, 4.69], [2.31, 8.68, 4.59]]
taskResource = [[3.94, 1.3, 13.9], [11.6, 2.8, 7.5], [5.43, 7.8, 52.68], [3.28, 1.5, 28.44], [6.05, 1.1, 44.81], [6.92, 2.3, 31.71], [4.37, 0.7, 32.57], [5.25, 6.9, 23.13], [4.38, 3, 40.19], [
    8.03, 1, 66.58], [3.41, 1.8, 7.43], [2.86, 1.5, 15.91], [2.96, 3.7, 43.31], [5.35, 4.1, 55.9], [4.11, 2.3, 5.4], [6.86, 7.4, 53.9], [4.46, 2.1, 46.8], [2.92, 0.5, 3.35], [4.34, 0.2, 1.86], [4.14, 2.5, 31.13]]


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


def generatePop(nodeN, pods, popN):
    pops = []
    for i in range(popN):
        pop = []
        for j in range(len(pods)):
            pop.append(random.randint(0, nodeN-1))
        pops.append(pop)
    return pops


def getFit(pods, pops, nodeN, nodeLoad):
    fitness = []
    for pop in pops:
        load = [[0, 0, 0] for i in range(nodeN)]
        for i in range(len(pop)):
            load[pop[i]][0] += pods[i].cpu
            load[pop[i]][1] += pods[i].mem
            load[pop[i]][2] += pods[i].net
        for i in range(len(load)):
            load[i][0] += nodeLoad[i][0]
            load[i][1] += nodeLoad[i][1]
            load[i][2] += nodeLoad[i][2]
        cpunorm = np.asarray([l[0] for l in load])
        memnorm = np.asarray([l[1] for l in load])
        netnorm = np.asarray([l[2] for l in load])
        loadbalance = 0.0
        for i in range(len(cpunorm)):
            if cpunorm[i] >= 100 or memnorm[i] >= 100:
                loadbalance = 999
        if loadbalance == 0.0:
            for i in range(len(cpunorm)):
                for j in range(i+1, len(cpunorm)):
                    loadbalance += abs(cpunorm[i] - cpunorm[j])
                    loadbalance += abs(memnorm[i] - memnorm[j])
                    loadbalance += abs(netnorm[i] - netnorm[j])
        fitness.append(loadbalance)
    return fitness


def getBestSol(pods, pops, nodeN, nodeLoad, bestSol):
    for pop in pops:
        load = [[0, 0, 0] for i in range(nodeN)]
        for i in range(len(pop)):
            load[pop[i]][0] += pods[i].cpu
            load[pop[i]][1] += pods[i].mem
            load[pop[i]][2] += pods[i].net
        for i in range(len(load)):
            load[i][0] += nodeLoad[i][0]
            load[i][1] += nodeLoad[i][1]
            load[i][2] += nodeLoad[i][2]
        cpuload = np.asarray([l[0] for l in load])
        memload = np.asarray([l[1] for l in load])
        netload = np.asarray([l[2] for l in load])
        flag = True
        for c in cpuload:
            if c >= 100:
                flag = False
        for c in memload:
            if c >= 100:
                flag = False
        if flag:
            loadbalance = 0.0
            for i in range(len(cpuload)):
                for j in range(i+1, len(cpuload)):
                    loadbalance += abs(cpuload[i] - cpuload[j])
                    loadbalance += abs(memload[i] - memload[j])
                    loadbalance += abs(netload[i] - netload[j])
            if nodeN < bestSol['nodeN']:
                bestSol['nodeN'] = nodeN
                bestSol['loadbalance'] = loadbalance
                bestSol['sol'] = pop
            elif loadbalance < bestSol['loadbalance']:
                bestSol['nodeN'] = nodeN
                bestSol['loadbalance'] = loadbalance
                bestSol['sol'] = pop
    return bestSol


def selectPop(pops, fitness):
    fitness = np.array(fitness)
    probs = np.array([(fitness.sum() - i) for i in fitness])
    idx = np.random.choice(np.arange(len(pops)), size=len(
        pops), replace=True, p=(probs)/probs.sum())
    newPops = []
    for i in idx:
        newPops.append(pops[i])
    return newPops


def cross(population, PC, nodeN, mu=1):
    population = np.array(population)
    N = population.shape[0]
    V = population.shape[1]
    populationList = [i for i in range(N)]

    for _ in range(N):
        r = random.random()
        if r < PC:
            p1, p2 = random.sample(populationList, 2)
            beta = np.array([0] * V)
            randList = np.random.random(V)

            for j in range(V):
                if randList.any() <= 0.5:
                    beta[j] = (2.0 * randList[j]) ** (1.0 / (mu + 1))
                else:
                    beta[j] = (1.0 / (2.0 * (1 - randList[j]))
                               ) ** (1.0 / (mu + 1))

                # 随机选取两个个体
                old_p1 = population[p1, ]
                old_p2 = population[p2, ]
                # 交叉
                new_p1 = np.round(
                    0.5 * ((1 + beta) * old_p1 + (1 - beta) * old_p2))
                new_p2 = np.round(
                    0.5 * ((1 - beta) * old_p1 + (1 + beta) * old_p2))

                # 上下界判断
                new_p1 = np.max(np.vstack((new_p1, np.array([0] * V))), 0)
                new_p1 = np.min(
                    np.vstack((new_p1, np.array([nodeN-1] * V))), 0)

                new_p2 = np.max(np.vstack((new_p2, np.array([0] * V))), 0)
                new_p1 = np.min(
                    np.vstack((new_p1, np.array([nodeN-1] * V))), 0)
                # 将交叉后的个体返回给种群
                population[p1, ] = new_p1
                population[p2, ] = new_p2
    return population.tolist()


def variation(pop, PM, nodeN, eta_m):
    for i in range(len(pop)):
        if np.random.rand() < PM:
            y = pop[i]
            ylow = 0
            yup = nodeN - 1
            delta1 = 1.0*(y-ylow)/(yup-ylow)
            delta2 = 1.0*(yup-y)/(yup-ylow)
            #delta=min(delta1, delta2)
            r = np.random.rand()
            mut_pow = 1.0/(eta_m+1.0)
            if r <= 0.5:
                xy = 1.0-delta1
                val = 2.0*r+(1.0-2.0*r)*(xy**(eta_m+1.0))
                deltaq = val**mut_pow-1.0
            else:
                xy = 1.0-delta2
                val = 2.0*(1.0-r)+2.0*(r-0.5)*(xy**(eta_m+1.0))
                deltaq = 1.0-val**mut_pow
            y = round(y+deltaq*(yup-ylow))
            y = min(yup, max(y, ylow))
            pop[i] = y
    return pop


def nodeDecrease(bestSol, nodeN, pods):
    if bestSol['nodeN'] > nodeN:
        return 0
    else:
        load = [[0, 0, 0] for i in range(nodeN)]
        pop = bestSol['sol']
        for i in range(len(pop)):
            load[pop[i]][0] += pods[i].cpu
            load[pop[i]][1] += pods[i].mem
            load[pop[i]][2] += pods[i].net
        for i in range(len(load)):
            load[i][0] += nodeLoad[i][0]
            load[i][1] += nodeLoad[i][1]
            load[i][2] += nodeLoad[i][2]
        flag = True
        for l in load:
            if l[0] >= 40 or l[1] >= 40:
                flag = False
        if flag:
            return 2
        else:
            return 1


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

bestSol = {}

nodeN = math.ceil(float(len(pods)) / float(podPerNode))
bestSol['nodeN'] = nodeN
bestSol['loadbalance'] = 1e9
bestSol['sol'] = [0 for i in pods]
flag = True
while flag and nodeN > 1:
    pops = generatePop(nodeN, pods, popN)
    for i in range(Ngeneratios):
        fit = getFit(pods, pops, nodeN, nodeLoad)
        newPops = selectPop(pops, fit)
        newPops = cross(newPops, PC, nodeN)
        for i in range(len(newPops)):
            newPops[i] = variation(newPops[i], PM, nodeN, eta_m)
        bestSol = getBestSol(pods, newPops, nodeN, nodeLoad, bestSol)
        print(bestSol)
    nodeDN = nodeDecrease(bestSol, nodeN, pods)
    if nodeDN == 0:
        flag = False
    else:
        nodeN -= nodeDN


print(bestSol)

load = [[0, 0, 0] for i in range(bestSol['nodeN'])]
pop = bestSol['sol']
for i in range(len(pop)):
    load[pop[i]][0] += pods[i].cpu
    load[pop[i]][1] += pods[i].mem
    load[pop[i]][2] += pods[i].net
for i in range(len(load)):
    load[i][0] += nodeLoad[i][0]
    load[i][1] += nodeLoad[i][1]
    load[i][2] += nodeLoad[i][2]
cpuload = np.asarray([l[0] for l in load])
memload = np.asarray([l[1] for l in load])
netload = np.asarray([l[2] for l in load])
print(cpuload)
print(memload)
print(netload)
