package org.wso2.carbon.sp.jobmanager.core.allocation;


import com.zaxxer.hikari.HikariDataSource;
import org.apache.log4j.Logger;
import org.wso2.carbon.datasource.core.api.DataSourceService;
import org.wso2.carbon.datasource.core.exception.DataSourceException;
import org.wso2.carbon.sp.jobmanager.core.appcreator.DistributedSiddhiQuery;
import org.wso2.carbon.sp.jobmanager.core.appcreator.SiddhiQuery;
import org.wso2.carbon.sp.jobmanager.core.bean.DeploymentConfig;
import org.wso2.carbon.sp.jobmanager.core.exception.ResourceManagerException;
import org.wso2.carbon.sp.jobmanager.core.impl.RDBMSServiceImpl;
import org.wso2.carbon.sp.jobmanager.core.internal.ServiceDataHolder;
import org.wso2.carbon.sp.jobmanager.core.model.ResourceNode;
import org.wso2.carbon.sp.jobmanager.core.model.ResourcePool;
import org.wso2.carbon.sp.jobmanager.core.model.SiddhiAppHolder;
//port org.wso2.siddhi.query.api.SiddhiApp;

import javax.sql.DataSource;
import java.sql.*;
import java.util.*;

/**
 * The Algorithm which evluate the allocation using the metrics of Partial SiddhiApps.
 */

public class MetricsBasedAllocationAlgorithm implements ResourceAllocationAlgorithm {

    private static final Logger logger = Logger.getLogger(MetricsBasedAllocationAlgorithm.class);
    private DeploymentConfig deploymentConfig = ServiceDataHolder.getDeploymentConfig();
    private Iterator resourceIterator;
    public Map<ResourceNode, List<PartialSiddhiApp>> output_map = new HashMap<>();
    public Map<String, Double> latency_map = new HashMap<>();
    public boolean check = false;
    int metricCounter;


    public Connection dbConnector() {
        try {
            String datasourceName = ServiceDataHolder.getDeploymentConfig().getDatasource();
            DataSourceService dataSourceService = ServiceDataHolder.getDataSourceService();
            DataSource datasource = (HikariDataSource) dataSourceService.getDataSource(datasourceName);
            Connection connection = datasource.getConnection();
            // Statement statement = connection.createStatement();
            // return statement;
            return connection;
        } catch (SQLException e) {
            logger.error("SQL error : " + e.getMessage());
        } catch (DataSourceException e) {
            logger.error("Datasource error : " + e.getMessage());
        }
        return null;
    }

    private List<SiddhiAppHolder> getSiddhiAppHolders(DistributedSiddhiQuery distributedSiddhiQuery) {
        List<SiddhiAppHolder> siddhiAppHolders = new ArrayList<>();
        distributedSiddhiQuery.getQueryGroups().forEach(queryGroup -> {
            queryGroup.getSiddhiQueries().forEach(query -> {
                siddhiAppHolders.add(new SiddhiAppHolder(distributedSiddhiQuery.getAppName(),
                        queryGroup.getGroupName(), query.getAppName(), query.getApp(),
                        null, queryGroup.isReceiverQueryGroup(), queryGroup.getParallelism()));
            });
        });
        return siddhiAppHolders;
    }
//mvn clean  install -DskipTests -Dfindbugs.skip=true

    public void retrieveData(DistributedSiddhiQuery distributedSiddhiQuery,
                             LinkedList<PartialSiddhiApp> partailSiddhiApps) {
        List<SiddhiAppHolder> appsToDeploy = getSiddhiAppHolders(distributedSiddhiQuery);
        Connection connection = dbConnector();
        Statement statement = null;
        try {
            statement = connection.createStatement();
        } catch (SQLException e) {
            logger.error("Error Creating the Connection", e);
        }
        ResultSet resultSet;
        try {
            for (SiddhiAppHolder appHolder : appsToDeploy) {
                String[] SplitArray = appHolder.getAppName().split("-");

                int executionGroup = Integer.valueOf(SplitArray[SplitArray.length - 2].substring(5));

                //executionGroup = Integer.valueOf(appHolder.getAppName().substring(appHolder.getAppName().length()-3,
                //appHolder.getAppName().length()-2));

                int parallelInstance = Integer.valueOf(SplitArray[SplitArray.length - 1]);

                //parallelInstance = Integer.valueOf(appHolder.getAppName().substring(appHolder.getAppName().length()-1));


                logger.info("Metric details of " + appHolder.getAppName() + "\n");
                logger.info("---------------------------------------------------");
                String query = "SELECT m3 ,m5, m7 ,m16 FROM metricstable where exec=" +
                        executionGroup + " and parallel=" + parallelInstance +
                        " order by iijtimestamp desc limit 1 ";
                resultSet = statement.executeQuery(query);

                logger.info("Query " + query);

                if (resultSet.isBeforeFirst()) {     //Check the corresponding partial siddhi app is having the metrics
                    logger.info("Matrics details are found for the partial Siddhi App");
                    while (resultSet.next()) {

                        double throughput = resultSet.getDouble("m3");
                        logger.info("Throughput : " + throughput);

                        int eventCount = resultSet.getInt("m5");
                        logger.info("Event Count : " + eventCount);

                        double latency = resultSet.getLong("m7");
                        logger.info("latency : " + latency);
                        latency_map.put(appHolder.getAppName(), latency);

                        double processCPU = resultSet.getDouble("m16");
                        logger.info("process CPU : " + processCPU);
                        if(latency != 0.0){
                            throughput= 1/latency;
                        }else{
                            logger.info("latency=0@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@");
                        }

                        partailSiddhiApps.add(new PartialSiddhiApp(processCPU, (latency), throughput, eventCount, appHolder.getAppName()));
                        logger.info(appHolder.getAppName() + " created with reciprocal of latency : " + (1 / latency) +
                                " and processCPU : " + processCPU + "\n");
                    }
                } else {
                    logger.warn("Metrics are not available for the siddhi app " + appHolder.getAppName()
                            + ". Hence using 0 as knapsack parameters");
                    metricCounter++;
                    double latency = 0.0;
                    logger.info("latency : " + latency);

                    double throughput = 0.0;
                    logger.info("Throughput = " + throughput);

                    double processCPU = 0.0;
                    logger.info("process CPU : " + processCPU);

                    int eventCount = 0;
                    logger.info("event count : " + eventCount);
                    partailSiddhiApps.add(new PartialSiddhiApp(processCPU, latency, throughput, eventCount, appHolder.getAppName()));
                    logger.info(appHolder.getAppName() + " created with reciprocal of latency : " + 0.0 +
                            ", Throughput :" + throughput +
                            " and processCPU : " + processCPU + "\n");
                }
                resultSet.close();

            }
            statement.close();
            connection.close();

            if ((metricCounter + 2) > appsToDeploy.size()) {
                logger.error("Metrics are not available for required number of Partial siddhi apps");
            }
        } catch (SQLException e) {
            logger.error(e);

        }
    }
    public ResourceNode getNextResourceNode(Map<String, ResourceNode> resourceNodeMap,
                                            int minResourceCount,
                                            SiddhiQuery siddhiQuery) {

        long initialTimestamp = System.currentTimeMillis();
        logger.info("Trying to deploy " + siddhiQuery.getAppName());
        if (deploymentConfig != null && !resourceNodeMap.isEmpty()) {
            if (resourceNodeMap.size() >= minResourceCount) {
                check = true;
                ResourceNode resourceNode = null;
                try {
                    logger.info("outmapsize in getNextResourcNode :" + output_map.size());
                    for (ResourceNode key : output_map.keySet()) {
                        for (PartialSiddhiApp partialSiddhiApp : output_map.get(key)) {
                            if (partialSiddhiApp.getName() == siddhiQuery.getAppName()) {
                                resourceNode = key;
                                return resourceNode;
                            }
                        }
                    }
                    if (resourceNode == null) {
                        if (resourceIterator == null) {
                            resourceIterator = resourceNodeMap.values().iterator();
                        }

                        if (resourceIterator.hasNext()) {
                            logger.warn(siddhiQuery.getAppName() + " did not allocatd in MetricsBasedAlgorithm ." +
                                    "hence deploying in " + (ResourceNode) resourceIterator.next());
                            return (ResourceNode) resourceIterator.next();
                        } else {
                            resourceIterator = resourceNodeMap.values().iterator();
                            if (resourceIterator.hasNext()) {
                                logger.warn(siddhiQuery.getAppName() + " did not allocatd in MetricsBasedAlgorithm ." +
                                        "hence deploying in " + (ResourceNode) resourceIterator.next());
                                return (ResourceNode) resourceIterator.next();
                            }
                        }
                    }
                } catch (ResourceManagerException e) {
                    if ((System.currentTimeMillis() - initialTimestamp) >= (deploymentConfig.
                            getHeartbeatInterval() * 2))
                        throw e;
                }
            }
        } else {
            logger.error("There are no enough resources to deploy");
        }
        return null;
    }

    public void executeKnapsack(Map<String, ResourceNode> resourceNodeMap,
                                int minResourceCount,
                                DistributedSiddhiQuery distributedSiddhiQuery) {
        logger.info("Inside New Knapsack ..............................................");

        if (deploymentConfig != null && !resourceNodeMap.isEmpty()) {
            if (resourceNodeMap.size() >= minResourceCount) {
                resourceIterator = resourceNodeMap.values().iterator();

                MultipleKnapsack multipleKnapsack = new MultipleKnapsack();

                LinkedList<PartialSiddhiApp> partialSiddhiApps = new LinkedList<>();
                retrieveData(distributedSiddhiQuery, partialSiddhiApps);

                logger.info("size of partialSiddhiApps list : " + partialSiddhiApps.size() + "\n");
                double TotalCPUUsagePartialSiddhi = 0.0;

                for (int j = 0; j < partialSiddhiApps.size(); j++) {
                    TotalCPUUsagePartialSiddhi = TotalCPUUsagePartialSiddhi + partialSiddhiApps.get(j).getcpuUsage();
                }

                logger.info("TotalCPUUsagePartialSiddhi : " + TotalCPUUsagePartialSiddhi + "\n");

                for (int p = 0; p < resourceNodeMap.size(); p++) {
                    ResourceNode resourceNode = (ResourceNode) resourceIterator.next();
                    multipleKnapsack.addKnapsack(new Knapsack((TotalCPUUsagePartialSiddhi / resourceNodeMap.size()),
                            resourceNode));

                    logger.info("created a knapsack of " + resourceNode);
                }
                logger.info("Starting branch and bound knapsack for partial siddhi apps....................");

                logger.info("before branch and bound :" + partialSiddhiApps.size());
                partialSiddhiApps = multipleKnapsack.executeBranchAndBoundKnapsack(partialSiddhiApps);

                logger.info("Size of Partial Siddhi apps after branch and bound....." + partialSiddhiApps.size());
                multipleKnapsack.calculatelatency();
                partialSiddhiApps = multipleKnapsack.printResult(false);
                multipleKnapsack.updatemap(output_map);
                logger.info("Remaining partial siddhi apps after completing the first branch and bound" + partialSiddhiApps.size());

                int i = 0;
                while (i < resourceNodeMap.size()) {

                    multipleKnapsack.greedyMultipleKnapsack(partialSiddhiApps);
                    multipleKnapsack.calculatelatency();
                    MultipleKnapsack result = multipleKnapsack.neighborSearch(multipleKnapsack);
                    logger.info("Results after " + "iteration " + (i + 1) + "\n");
                    logger.info("-----------------------------------\n");
                    multipleKnapsack.updatemap(output_map);
                    partialSiddhiApps = result.printResult(false);
                    logger.info("partialSiddhiappsize : " + partialSiddhiApps.size());
                    logger.info("outputmap size in MBA " + output_map.size());
                    i++;
                }
                if (partialSiddhiApps.size() > 0) {

                    ArrayList<Double> temp = new ArrayList<Double>();
                    for (int g = 0; g < multipleKnapsack.getKnapsacks().size(); g++) {
                        temp.add(multipleKnapsack.getKnapsacks().get(g).getcapacity());
                        logger.info(multipleKnapsack.getKnapsacks().get(g).getcapacity() + " added");
                    }
                    Collections.sort(temp);
                    int k = 1;
                    for (PartialSiddhiApp item : partialSiddhiApps) {
                        double s = temp.get(temp.size() - k);
                        //logger.info("s " +s);
                        for (int h = 0; h < multipleKnapsack.getKnapsacks().size(); h++) {
                            if (multipleKnapsack.getKnapsacks().get(h).getcapacity() == s) {
                                multipleKnapsack.getKnapsacks().get(h).addPartialSiddhiApps(item);

                            }
                        }
                        k++;

                    }

                    MultipleKnapsack result = multipleKnapsack.neighborSearch(multipleKnapsack);
                    multipleKnapsack.updatemap(output_map);
                    result.printResult(true);
                    logger.info("Remaining partial siddhi apps after complete iteration " + partialSiddhiApps.size());
                }


            } else {
                logger.error("Minimum resource requirement did not match, hence not deploying the partial siddhi app ");
            }
        }

    }
}