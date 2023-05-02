package com.simplerpa.cloudservice.utils;

import com.alibaba.fastjson.JSONObject;
import com.simplerpa.cloudservice.entity.util.DictionaryUtil;
import org.apache.commons.lang3.ArrayUtils;
import org.jfree.chart.ChartFactory;
import org.jfree.chart.ChartPanel;
import org.jfree.chart.JFreeChart;
import org.jfree.chart.plot.PlotOrientation;
import org.jfree.data.xy.XYSeries;
import org.jfree.data.xy.XYSeriesCollection;

import javax.swing.*;
import java.util.*;

public class ParticleSwarm {
    /**
     * 变量的个数
     * */
    public int nVar;

    public double sum;

    /**
     * 粒子的位置，包含解的信息
     * */
    public	double [] position;
    /**
     * 粒子的速度
     * */
    public	double [] velecity;
    /**
     * 粒子历史最有位置
     * */
    public	double [] P_position;
    /**
     * 粒子的历史最优适应值
     * */
    public	double P_fitness;
    /**
     * 粒子的适应值
     * */
    public	double fitness;
    /**
     * 粒子的运动边界 -- 下
     * */
    public 	double[] x_low;
    /**
     * 粒子的运动边界--上
     * */
    public 	double[] x_up;
    static Random rand1=new Random(System.nanoTime());   // 随机量生成
    // java 的构造函数（没有析构函数）
    //粒子的初始化
    /**
     输入变量：所求变量的个数，变量的下边界，上边界
     */
    ParticleSwarm(int nVar,double[] x_low,double[] x_up, int psoType, int T){
        position = new double[nVar];
        velecity = new double[nVar];
        P_position = new double[nVar];
        sum = 0;
        this.x_low=x_low;
        this.x_up=x_up;
        // 初始化在边界内：位置，速度
        for(int i=0;i<nVar;i++){
            int flag = rand1.nextBoolean()? 1 : -1;
            if(psoType == -1){
                position[i] = getRandPosition(1, 0);
                while(position[i] == 0 || position[i] == 1 || position[i] == 0.25 || position[i] == 0.75){
                    position[i] = getRandPosition(1, 0);
                }
                position[i] = PSO.chaosValue(position[i]);
                position[i] = x_low[i] + position[i] * (x_up[i] - x_low[i]);
            }else{
                position[i] = getRandPosition(x_up[i], x_low[i]) * flag;
            }
            velecity[i] = rand1.nextDouble() * flag;
        }
    }

    public static double getRandPosition(double up, double low){
        return rand1.nextDouble() * (up - low) + low;
    }

    public static double getR1(int i, int MaxIter){
//        return rand1.nextDouble();
        double no = (double)i / MaxIter;
        return (1 - no);
    }

    public static double getR2(int i, int MaxIter){
//        return rand1.nextDouble();
        double no = (double)i / MaxIter;
        return (0.01 + no);
    }
}

class WindowQueue{
    private final double[] list;
    private final int size;
    private int pos, remainSize;
    private double sum;
    private static final double MIN_NUM = 0.001, MAX_STEP = 1.49;
    public WindowQueue(int size){
        this.size = size;
        this.remainSize = size;
        this.pos = 0;
        this.sum = 0;
        this.list = new double[size];
        Arrays.fill(list, 0);
    }
    public void insertElement(double item){
        sum -= list[pos]*list[pos];
        sum += item*item;
        list[pos++] = item;
        pos %= size;
        if(remainSize > 0){
            remainSize--;
        }
    }
    public void insertOriginElement(double item){
        sum -= list[pos];
        sum += item;
        list[pos++] = item;
        pos %= size;
        if(remainSize > 0){
            remainSize--;
        }
    }
    public boolean isFull(){
        return remainSize == 0;
    }
    public double getElementAVG(){
        return sum / size;
    }
    public double getMinElement(){
        double minRes = list[0];
        for(int i=0; i<size; i++){
            minRes = Math.min(list[i], minRes);
            if(list[i] == 0){
                break;
            }
        }
        return minRes;
    }
}

class AdamDelta{
    /**
     * 每个粒子的维度列表
     * */
    WindowQueue[] list, globalList;
    WindowQueue fitnessWindow;
    public AdamDelta(int dimensionNum, int windowSize){
        list = new WindowQueue[dimensionNum];
        globalList = new WindowQueue[dimensionNum];
        for(int i=0; i<dimensionNum; i++){
            list[i] = new WindowQueue(windowSize);
            globalList[i] = new WindowQueue(windowSize);
        }
        fitnessWindow = new WindowQueue(windowSize);
    }
}

class BestFitSet{
    private Double fitness;
    private HashMap<String, double[]> hashMap;
    private int it;
    public static final Random rand = new Random();

    public BestFitSet(){
        fitness = -1.0;
        it = -1;
        hashMap = new HashMap<>();
    }

    public void setFitness(Double fitness, int it){
        this.fitness = fitness;
        hashMap = new HashMap<>();
        this.it = it;
    }

    public int getHashMapSize(){
        return hashMap.size();
    }

    public Double getFitness(){
        return fitness;
    }

    public boolean shouldChange(int it){
        if(hashMap.isEmpty()){
            this.it = it;
            return false;
        }
        return (it - this.it) % PSO.WindowSize == 0;
    }

    public double[] getRandPosition(){
        int i = rand.nextInt() % getHashMapSize();
        i = Math.abs(i);
        String s = (String) hashMap.keySet().toArray()[i];
        return hashMap.get(s);
    }

    public void putPosition(double[] position){
        StringBuilder str = new StringBuilder();
        for (double v : position) {
            str.append((int) Math.abs(v));
        }
        if(!hashMap.containsKey(str.toString())){
            hashMap.put(str.toString(), position);
        }
    }
}

/*
粒子群算法类
*/
class PSO{
    /**
     * 算法的学习因子
     * */
    public double c1, c2, R1, R2;

    /**
     * 服务器状态
     * */
    private final double[][] performanceList = new double[4][4];
    private final double[][] allocateInfoList = new double[4][4];
    private Long[] ids;

    /**
     * 惯性权重系数
     * */
    public double w;
    /**
     * 粒子群种群
     * */
    public ParticleSwarm[] pop;
    /**
     * 最大迭代次数
     * */
    public int MaxIter;
    /**
     * 种群数量
     * */
    public int nPop;
    public double[] x_low;	// 变量下边界
    public double[] x_up;		// 变量上边界
    /**
     * 全局最优适应值
     * */
    public double best_fitness, worstFitness;
    /**
     * 全局最优位置（最优解）
     * */
    public double[] best_solution, worst_solution;
    /**
     * 变量个数(评价函数的维度)
     * */
    public int nVar;

    /**
     * AdamDelta优化器
     * */
    public AdamDelta[] adamDeltas;

    /**
     * 散点图数据集
     * */
    XYSeriesCollection dataSet;

    private static boolean GRAPH_SHOW = false;

    public static final int WindowSize = 30, TEST_COUNT = 10;

    private int standard_pso;
    private final BestFitSet bestFitSet = new BestFitSet();
    private static final double Epsilon = 0.2;

    private static final double badParticleRate = 0.5;

    public static final int FIT_TYPE = 1;

    PSO(double[] x_low, double[] x_up, JSONObject mai, JSONObject mpj, Long[] ids, int isStandard, boolean graphShow){
//        this(1.49445, 1.49445, 0.729, ids.length, 200, 50, x_low, x_up);
        standard_pso = isStandard;
        try {
            for (int i = 0; i < 4; i++) {
                String machineName = TaskScheduleAllocator.machineName.get(i);
                double[] object = mai.getJSONObject(machineName).getObject(TaskCostCountUtil.LIST, double[].class);
                allocateInfoList[i] = object;

                Double Nc = mpj.getJSONObject(machineName).getDouble("cpu");
                Double Nm = mpj.getJSONObject(machineName).getDouble("mem");
                Double Nn = mpj.getJSONObject(machineName).getDouble("net");
                performanceList[i][0] = Nc;
                performanceList[i][1] = Nm;
                performanceList[i][2] = Nn;
            }
        }catch (Exception e){}
        this.ids = ids.clone();
        GRAPH_SHOW = graphShow;
        int SwarmNum = 50;
        initEveryParams(1.49445, 1.49445, 0.729, ids.length, 2000, SwarmNum, x_low, x_up);
    }

    PSO(double[] x_low, double[] x_up, JSONObject mai, JSONObject mpj, Long[] ids, int isStandard){
        this(x_low, x_up, mai, mpj, ids, isStandard, false);
    }

	/*
	构造函数初始化
	*/
    PSO(double c1,double c2,double w,int nVar,int MaxIter,int nPop,double[] x_low,double[] x_up){
        initEveryParams(c1, c2, w, nVar, MaxIter, nPop, x_low, x_up);
    }

    private void initEveryParams(double c1,double c2,double w,int nVar,int MaxIter,int nPop,double[] x_low,double[] x_up){
        this.c1=c1;
        this.c2=c2;
        this.w=w;
        this.nVar=nVar;
        this.MaxIter=MaxIter;
        this.nPop=nPop;
        this.x_low=x_low;
        this.x_up=x_up;
        this.pop = new ParticleSwarm[nPop];
        this.adamDeltas = new AdamDelta[nPop];
        this.R1 = ParticleSwarm.getR1(0, MaxIter);
        this.R2 = ParticleSwarm.getR2(0, MaxIter);

        // 种群初始化
        for(int i=0;i<nPop;i++){
            pop[i]=new ParticleSwarm(nVar,x_low,x_up,standard_pso, MaxIter);  				// 初始化每个粒子
            pop[i].fitness=function_fitness(pop[i].position);	// 计算每个粒子的适应值
            pop[i].P_fitness=pop[i].fitness;					// 初始化粒子的最优适应值
            pop[i].P_position=(double[])pop[i].position.clone();// 初始化粒子的最优位值，数组的复制用clone，
            adamDeltas[i] = new AdamDelta(nVar, WindowSize);

            if(i == 0){
                best_fitness=pop[i].fitness;
                best_solution=(double[])pop[i].position.clone();
            }else if(best_fitness > pop[i].fitness){
                best_fitness=pop[i].fitness;
                best_solution=(double[])pop[i].position.clone();
            }
        }
        bestFitSet.setFitness(best_fitness, 0);

        if(GRAPH_SHOW){
            initGraphUI();
        }
    }

    private void initGraphUI(){
        XYSeriesCollection dataSet = new XYSeriesCollection();
        JFreeChart freeChart = ChartFactory.createScatterPlot(
                "散点图",
                "X",
                "Y",
                dataSet,
                PlotOrientation.VERTICAL,
                true,
                true,
                false
        );
        ChartPanel chartPanel = new ChartPanel(freeChart);
        chartPanel.setPreferredSize(new java.awt.Dimension(560, 400));
        JFrame frame = new JFrame("饼图");
        frame.setLocation(500, 400);
        frame.setSize(600, 500);

        //将主窗口的内容面板设置为图表面板
        frame.setContentPane(chartPanel);

        frame.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
        frame.setVisible(true);
        this.dataSet = dataSet;
    }

    // 适应值函数
    public double function_fitness(double[] var){
//        return FitnessFuncLib.getFitFromFunc(var, FIT_TYPE);
//        return Sphere(var);
//        return Rosenbrock(var);
        return Schedule(var);
    }

    private double Sphere(double[] var){
        double sum=0;
        for(int i=0;i<var.length;i++)
        {
            sum+=var[i]*var[i];
        }
        return sum;
    }
    private double Rosenbrock(double[] var){
        double sum=0;
        for(int i=0;i<var.length-1;i++)
        {
            sum += 100*(var[i+1] - var[i]*var[i])*(var[i+1] - var[i]*var[i]) + (var[i]-1)*(var[i]-1);
        }
        return sum;
    }

    private double Schedule(double[] var){
        double[][] sumList = new double[4][3];
        double sum;
        double F = 0, G = 0, T[] = new double[TaskScheduleAllocator.machineName.size()];
        double maxBalance = -1;
        for(int i=0; i<nVar; i++){
            int targetMachine = (int) Math.abs(var[i]);
            if(targetMachine > 3){
                targetMachine = 3;
            }
            List<Double> costListByTaskId = TaskCostCountUtil.getCostListByTaskId(ids[i]);
            sumList[targetMachine][0] += costListByTaskId.get(0); // cpu
            sumList[targetMachine][1] += costListByTaskId.get(1); // mem
            sumList[targetMachine][2] += costListByTaskId.get(2); // net
            T[targetMachine] += costListByTaskId.get(3);
        }
        for(int i=0; i<4; i++){
            double[] machineCostList = allocateInfoList[i];
            sumList[i][0] += machineCostList[0];
            sumList[i][1] += machineCostList[1];
            sumList[i][2] += machineCostList[2];

            sumList[i][1] = TaskCostCountUtil.getMemCost(i, sumList[i][1]);
            double Nc, Nm, Nn;
            double Rc, Rm, Rn;
            Nc = performanceList[i][0];
            Nm = performanceList[i][1];
            Nn = performanceList[i][2];
            Rc = 100-Nc;
            Rm = 100-Nm;
            Rn = 100-Nn;

            Rc = DictionaryUtil.checkValueAndChange(sumList[i][0]/Rc);
            Rm = DictionaryUtil.checkValueAndChange(sumList[i][1]/Rm);
            Rn = DictionaryUtil.checkValueAndChange(sumList[i][2]/Rn);
            Nc = DictionaryUtil.checkValueAndChange(sumList[i][0] + Nc);
            Nm = DictionaryUtil.checkValueAndChange(sumList[i][1] + Nm);
            Nn = DictionaryUtil.checkValueAndChange(sumList[i][2] + Nn);

            F += Math.sqrt(Rc*Rc + Rm*Rm + Rn*Rn)*T[i];
            G += Math.sqrt(Nc*Nc + Nm*Nm + Nn*Nn);
            maxBalance = Math.max(maxBalance, Math.sqrt(Nc*Nc + Nm*Nm + Nn*Nn));
        }
        sum = DictionaryUtil.F_VAL*F + 0.5*DictionaryUtil.G_VAL*(G + maxBalance*TaskScheduleAllocator.machineName.size());
        return sum;
    }

    /*
    种群搜索过程，粒子更新的方法
    1.先计算粒子的速度，按公式计算，采用基本粒子群算法的更新公式
    2.对出界的速度进行限制
    3.按公式更新粒子的位置
    4.对出界的位置进行限制
    */
    public void up_search(int it){
        for(int i=0;i<nPop;i++) {
            for (int j = 0; j < nVar; j++) {
                pop[i].velecity[j] = w * pop[i].velecity[j] + R1 * (pop[i].P_position[j] - pop[i].position[j]) * getC1(it, MaxIter) + R2 * (best_solution[j] - pop[i].position[j]) * getC2(it, MaxIter);

                if (Math.abs(pop[i].velecity[j]) < 0.1) {
                    if (pop[i].velecity[j] > 0) {
                        pop[i].velecity[j] = 0.1;
                    } else {
                        pop[i].velecity[j] = -0.1;
                    }
                }

                if (Math.abs(pop[i].velecity[j]) > (x_up[j] - x_low[j]) * 0.1) {
                    if (pop[i].velecity[j] > 0) {
                        pop[i].velecity[j] = 0.1;
                    } else {
                        pop[i].velecity[j] = -(x_up[j] - x_low[j]) * 0.1;
                    }
                }

                pop[i].position[j] = pop[i].position[j] + pop[i].velecity[j];

                if (Double.isNaN(pop[i].position[j])) {
                    pop[i].position[j] = 0;
                }

                if(Double.isNaN(pop[i].velecity[j])){
                    pop[i].velecity[j] = BestFitSet.rand.nextGaussian();
                }

                if (pop[i].position[j] > x_up[j]) {
                    pop[i].position[j] = x_up[j];
                }
                if (pop[i].position[j] < x_low[j]) {
                    pop[i].position[j] = x_low[j];
                }
            }
        }
    }
    //更新适应值
    public void up_date(int it){
        for(int i=0;i<this.nPop;i++){
            //计算适应值
            pop[i].fitness = function_fitness(pop[i].position);
//            worstFitness = Math.max(pop[i].fitness, worstFitness);
            if(worstFitness < pop[i].fitness){
                worstFitness = pop[i].fitness;
                worst_solution = pop[i].position;
            }

            if(pop[i].fitness == best_fitness){
                bestFitSet.putPosition(pop[i].position.clone());
            }

         // 如果个体的适应值大于个体历史最优适应值，则更新个体历史最优适应值，位置信息同样的也更新
            if(pop[i].fitness < pop[i].P_fitness){
                pop[i].P_position = pop[i].position.clone();
                pop[i].P_fitness = pop[i].fitness;
                // 如果个体的适应值比全局的适应值优，则更新全局的适应值和位置
                if(pop[i].fitness < best_fitness)
                {
                    best_fitness = pop[i].fitness;
                    best_solution = pop[i].position.clone();
                    if(standard_pso == 0){
                        bestFitSet.setFitness(best_fitness, it);
                        bestFitSet.putPosition(best_solution.clone());
                    }
                }
            }

            if(standard_pso == -1){
                double[] clone = best_solution.clone();
                for (int z=0; z<clone.length; z++){
                    clone[z] = (clone[z] - x_up[0]) / (x_up[0] - x_low[0]);
                    clone[z] = chaosValue(clone[z]);
                    clone[z] = clone[z] * (x_up[0] - x_low[0]) + x_low[0];
                    clone[z] = clone[z] * getEpsilon(it, MaxIter);
                }
                double v = function_fitness(clone);
                if(v < best_fitness){
                    best_fitness = v;
                    best_solution = clone;
                }
            } else if(standard_pso == 0){
                adamDeltas[i].fitnessWindow.insertOriginElement(pop[i].fitness);
                if(adamDeltas[i].fitnessWindow.isFull() && (adamDeltas[i].fitnessWindow.getMinElement() > pop[i].P_fitness)){
//                    for(int j=0; j<nVar; j++){
//                        pop[i].position[j] += (pop[i].velecity[j])*BestFitSet.rand.nextDouble();
//                        if(pop[i].position[j]>x_up[j]){
//                            pop[i].position[j]=x_up[j];
//                        }
//                        if(pop[i].position[j]<x_low[j]){
//                            pop[i].position[j]=x_low[j];
//                        }
//                    }

//                    int[] tailPosList = generateTailPos(pop[i].position);
                    int[] tailPosList = generateTailPos(pop[i]);
                    for (int j : tailPosList) {
                        pop[i].position[j] = (x_up[j] - x_low[j]) * BestFitSet.rand.nextDouble() + x_low[j];
                    }
                    pop[i].P_fitness = function_fitness(pop[i].position);
                    adamDeltas[i].fitnessWindow = new WindowQueue(WindowSize);
                }
            }
        }
        if(standard_pso == 0 && bestFitSet.shouldChange(it)){
            best_fitness = bestFitSet.getFitness();
            best_solution = bestFitSet.getRandPosition().clone();
        }
    }

    private int[] generateTailPos(double[] var){
        double[] clone;
        TreeMap<Double, Integer> treeMap = new TreeMap<>();
        for(int i=0; i<var.length; i++){
            clone = ArrayUtils.remove(var, i);
            treeMap.put(function_fitness(clone), i);
        }
        int num = (int) Math.ceil((nVar*badParticleRate));
        int[] list = new int[num];
        for (Map.Entry<Double, Integer> item : treeMap.entrySet()){
            if(num <= 0){
                break;
            }
            list[num-1] = item.getValue();
            num--;
        }
        return list;
    }

    private int[] generateTailPos(ParticleSwarm particleSwarm){
        double[] var = particleSwarm.position, T = new double[TaskScheduleAllocator.machineName.size()];
        double[][] sumList = new double[4][3];
        double[][] costList = new double[4][3];
        for(int i=0; i<nVar; i++){
            int targetMachine = (int) Math.abs(var[i]);
            if(targetMachine > 3){
                targetMachine = 3;
            }
            List<Double> costListByTaskId = TaskCostCountUtil.getCostListByTaskId(ids[i]);
            costList[targetMachine][0] += costListByTaskId.get(0); // cpu
            costList[targetMachine][1] += costListByTaskId.get(1); // mem
            costList[targetMachine][2] += costListByTaskId.get(2); // net
            T[targetMachine] += costListByTaskId.get(3);
        }
        for(int i=0; i<4; i++){
            double[] machineCostList = allocateInfoList[i];
            sumList[i][0] += machineCostList[0];
            sumList[i][1] += machineCostList[1];
            sumList[i][2] += machineCostList[2];
        }
        TreeMap<Double, Integer> treeMap = new TreeMap<>();
        for(int z=0; z<nVar; z++){
            double F = 0, G = 0;
            List<Double> costListByTaskId = TaskCostCountUtil.getCostListByTaskId(ids[z]);
            double maxBalance = -1;
            for(int i=0; i<4; i++){
                double memSum = TaskCostCountUtil.getMemCost(i, sumList[i][1] + costList[i][1] - costListByTaskId.get(1)),
                        cpuSum = sumList[i][0] + costList[i][0] - costListByTaskId.get(0),
                        netSum = sumList[i][2] + costList[i][2] - costListByTaskId.get(2);
                double Nc, Nm, Nn;
                double Rc, Rm, Rn;
                Nc = performanceList[i][0];
                Nm = performanceList[i][1];
                Nn = performanceList[i][2];
                Rc = 100-Nc;
                Rm = 100-Nm;
                Rn = 100-Nn;

                Rc = DictionaryUtil.checkValueAndChange(cpuSum/Rc);
                Rm = DictionaryUtil.checkValueAndChange(memSum/Rm);
                Rn = DictionaryUtil.checkValueAndChange(netSum/Rn);
                Nc = DictionaryUtil.checkValueAndChange(cpuSum + Nc);
                Nm = DictionaryUtil.checkValueAndChange(memSum + Nm);
                Nn = DictionaryUtil.checkValueAndChange(netSum + Nn);

                F += Math.sqrt(Rc*Rc + Rm*Rm + Rn*Rn)*(T[i] - costListByTaskId.get(3));
                G += Math.sqrt(Nc*Nc + Nm*Nm + Nn*Nn);
                maxBalance = Math.max(maxBalance, Math.sqrt(Nc*Nc + Nm*Nm + Nn*Nn));
            }
            double FG = (F * DictionaryUtil.F_VAL) + 0.5*DictionaryUtil.G_VAL*(G + maxBalance*TaskScheduleAllocator.machineName.size());
            treeMap.put(FG, z);
        }
        int num = (int) Math.ceil((nVar*badParticleRate));
        int[] list = new int[num];
        for (Map.Entry<Double, Integer> item : treeMap.entrySet()){
            if(num <= 0){
                break;
            }
            list[num-1] = item.getValue();
            num--;
        }
        return list;
    }

    public void getFAndG(double[] var){
        double[][] sumList = new double[4][3];
        double sum;
        double F = 0, G = 0, T[] = new double[TaskScheduleAllocator.machineName.size()];
        double maxBalance = -1;
        for(int i=0; i<nVar; i++){
            int targetMachine = (int) Math.abs(var[i]);
            if(targetMachine > 3){
                targetMachine = 3;
            }
            List<Double> costListByTaskId = TaskCostCountUtil.getCostListByTaskId(ids[i]);
            sumList[targetMachine][0] += costListByTaskId.get(0); // cpu
            sumList[targetMachine][1] += costListByTaskId.get(1); // mem
            sumList[targetMachine][2] += costListByTaskId.get(2); // net
            T[targetMachine] += costListByTaskId.get(3);
        }
        for(int i=0; i<4; i++){
            double[] machineCostList = allocateInfoList[i];
            sumList[i][0] += machineCostList[0];
            sumList[i][1] += machineCostList[1];
            sumList[i][2] += machineCostList[2];

            sumList[i][1] = TaskCostCountUtil.getMemCost(i, sumList[i][1]);
            double Nc, Nm, Nn;
            double Rc, Rm, Rn;
            Nc = performanceList[i][0];
            Nm = performanceList[i][1];
            Nn = performanceList[i][2];
            Rc = 100-Nc;
            Rm = 100-Nm;
            Rn = 100-Nn;

            Rc = DictionaryUtil.checkValueAndChange(sumList[i][0]/Rc);
            Rm = DictionaryUtil.checkValueAndChange(sumList[i][1]/Rm);
            Rn = DictionaryUtil.checkValueAndChange(sumList[i][2]/Rn);
            Nc = DictionaryUtil.checkValueAndChange(sumList[i][0] + Nc);
            Nm = DictionaryUtil.checkValueAndChange(sumList[i][1] + Nm);
            Nn = DictionaryUtil.checkValueAndChange(sumList[i][2] + Nn);

            F += Math.sqrt(Rc*Rc + Rm*Rm + Rn*Rn)*T[i];
            G += Math.sqrt(Nc*Nc + Nm*Nm + Nn*Nn);
            maxBalance = Math.max(maxBalance, Math.sqrt(Nc*Nc + Nm*Nm + Nn*Nn));
        }
        System.out.println("cost: " + F);
        System.out.println("balance: " + G);
        System.out.println("maxBalance: " + TaskScheduleAllocator.machineName.size()*maxBalance);
    }

    // 显示结果，显示每一次迭代计算后的最优适应值
    public void show_result(int Iter_c){
        System.out.printf("Iteration: %3d , global best fit:%5f\n",Iter_c,best_fitness);
        if(Iter_c==(MaxIter-1))
        {
            for(int i=0;i<nVar;i++){
                System.out.println(best_solution[i]);
            }
//            System.out.println("The PSO end ,plase look up the result if need!");
        }
    }
    // PSO 程序开始运行
    public JSONObject run()
    {
//        up_date();
        // 按照设置的最大迭代次数迭代计算
        for(int it =0;it<MaxIter;it++){
//            this.R1 = ParticleSwarm.getR1(it, MaxIter);
//            this.R2 = ParticleSwarm.getR2(it, MaxIter);
            this.R1 = ParticleSwarm.rand1.nextDouble();
            this.R2 = ParticleSwarm.rand1.nextDouble();
            this.w = getW(it, MaxIter);

            up_search(it);	// 速度位置更新
            up_date(it);		// 适应值的更新
//            show_result(it);  // 输出结果的显示

            try{
                if(GRAPH_SHOW){
                    MainPSO.outputPSO(this, dataSet);
                }
            }catch (Exception e){
                e.printStackTrace();
            }
        }
        JSONObject jsonObject = new JSONObject();
        jsonObject.put("best_fit", best_fitness);
        jsonObject.put("best_sov", best_solution);
        return jsonObject;
    }

    private double getC1(int i, int j){
//        double c1 = adamDeltas[i].list[j].getParamsRes();
        if(standard_pso == -1){
            double cmax = 2.5, cmin = 0.5;
            return 0.5*(cmax - cmin) * ((double) i / j) * ((double) i / j)  + cmin;
        }else if(standard_pso == -2){
            double cmax = 2.5, cmin = 1.5;
            return cmax - Math.pow((1 - (Math.exp(1) / (Math.exp(1) - Math.exp(1.0 / j)))) + (1/(Math.exp(1) - Math.exp(1.0/j)))*Math.exp((double) i/j), 0.5)*(cmax - cmin);
        }
        return c1;
    }

    private double getC2(int i, int j){
//        double c2 = adamDeltas[i].globalList[j].getParamsRes();
        if(standard_pso == -1){
            double cmax = 2.5, cmin = 0.5;
            return (cmax - cmin) * ((double) i / j) * ((double) i / j)  + cmax;
        }else if(standard_pso == -2){
            double cmax = 2.5, cmin = 1.5;
            return cmin + Math.pow((1 - (Math.exp(1) / (Math.exp(1) - Math.exp(1.0 / j)))) + (1/(Math.exp(1) - Math.exp(1.0/j)))*Math.exp((double) i/j), 0.5)*(cmax - cmin);
        }
        return c2;
    }

    private double getW(int k, int T){
        double w = this.w;
        if(standard_pso == -1){
            double wmax = 0.9, wmin = 0.4;
            double percent = (double) k / T;
            if(percent <= 0.1){
                w = wmax;
            }else{
                w = wmin + (wmax + wmin) / (Math.exp(-wmax) + Math.exp(-1.2 + 20*k/(double) T));
            }
        }else if(standard_pso == -2){
            double wmax=0.8, wmin=0.001;
            w = (wmax - wmin)*((T - k)/(double)(T - 1)) + wmin;
        }
        return w;
    }

    public static double chaosValue(double num){
        return 4 * num * (1 - num);
    }

    public static double getEpsilon(int k, int T){
        return Epsilon + 1 / (Math.exp(0.1 + 5*k/(double)T));
    }
}

// 定义主类进行计算
class MainPSO{
    public static void main(String[] args){
        System.out.println(PSO.FIT_TYPE);
        System.out.println("The PSO START....");
        stdTest(0);
        System.out.println("The PSO END....");
        System.out.println("The ChPSO START....");
        stdTest(-2);
        System.out.println("The ChPSO END....");
        System.out.println("The CAPSO START....");
        stdTest(-1);
        System.out.println("The CAPSO END....");
    }

    public static void stdTest(int isStd){
        // 初始赋值
        double[] sum = new double[2];
        double smallestNum = 1e20;
        sum[0] = 0;
        sum[1] = 0;

        double[] x_low = FitnessFuncLib.getLow(PSO.FIT_TYPE), x_up = FitnessFuncLib.getUp(PSO.FIT_TYPE);
        Long[] ids = new Long[x_low.length];

        ArrayList<Double> list = new ArrayList<>();

        for(int i = 0; i < PSO.TEST_COUNT; i++){
//            System.out.println("The PSO START....");
//            long st = System.currentTimeMillis();
            PSO pso = new PSO(x_low, x_up, new JSONObject(), new JSONObject(), ids, isStd);
            JSONObject run = pso.run();
//            long et = System.currentTimeMillis();
//            System.out.println(et - st);
            list.add(run.getDouble("best_fit"));
            smallestNum = Math.min(run.getDouble("best_fit"), smallestNum);
//            System.out.println("The PSO END....");
//            System.out.println("worstFitness : " + pso.worstFitness);
        }
        System.out.println("bestFit(mean): " + DictionaryUtil.getAvgByList(list));
        System.out.println("bestFit(std): " + DictionaryUtil.getStdDevByList(list));
        System.out.println("bestFit: " + smallestNum);
    }

    public static void outputPSO(PSO pso, XYSeriesCollection dataSet) throws InterruptedException {
        XYSeries pointData = new XYSeries("data");
        for(int j=0; j<pso.nPop; j++){
            pointData.add(pso.pop[j].position[0], pso.pop[j].position[1]);
        }
        pointData.add(pso.x_low[0], pso.x_low.length == 1 ? 0 : pso.x_low[1]);
        pointData.add(pso.x_up[0], pso.x_up.length == 1 ? 0 : pso.x_up[1]);
        dataSet.removeAllSeries();
        dataSet.addSeries(pointData);
        Thread.sleep(50);
    }
}