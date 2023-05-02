import numpy as np
import random

"""
    SBX 模拟二进制交叉
    SBX主要是模拟基于二进制串的单点交叉工作原理,将其作用于以实数表示的染色体。
    两个父代染色体经过交叉操作产生两个子代染色体,使得父代染色体的有关模式信息在子代染色体中得以保留。
    输入：
        population 种群规模
        alfa 交叉概率
        numRangeList 决策变量上限
        mu是一个(0,1)的随机数
"""


def cross(population, alfa, numRangeList, mu=1):
    N = population.shape[0]
    V = population.shape[1]
    populationList = range(N)

    for _ in range(N):
        r = random.random()

        if r < alfa:
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
                new_p1 = 0.5 * ((1 + beta) * old_p1 + (1 - beta) * old_p2)
                new_p2 = 0.5 * ((1 - beta) * old_p1 + (1 + beta) * old_p2)

                # 上下界判断
                new_p1 = np.max(np.vstack((new_p1, np.array([0] * V))), 0)
                new_p1 = np.min(np.vstack((new_p1, numRangeList)), 0)

                new_p2 = np.max(np.vstack((new_p2, np.array([0] * V))), 0)
                new_p2 = np.min(np.vstack((new_p2, numRangeList)), 0)

                # 将交叉后的个体返回给种群
                population[p1, ] = new_p1
                population[p2, ] = new_p2


if __name__ == '__main__':
    random.seed(0)
    np.random.seed(0)
    xN = 10
    yN = 5
    alfa = 0.9
    population = np.random.rand(xN * yN).reshape(xN, yN) * 1.0

    print('交叉前：')
    print(population)
    # 交叉
    cross(population, alfa, np.array([1] * 5))
    print('交叉后：')
    print(population)
