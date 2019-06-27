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
        double currentSummeryThroughput = 0.0;

        try {
            logger.info("Autocommit enabled ? "+connection.getAutoCommit());
            logger.info("Client information" +connection.getClientInfo());
            connection.setAutoCommit(true);
            statement = connection.createStatement();
        } catch (SQLException e) {
            logger.error("Error Creating the Connection", e);
        }
        ResultSet resultSet;
        try {
            for (SiddhiAppHolder appHolder : appsToDeploy) {
                logger.info("Starting partial Siddhi app ......................." + appHolder.getAppName());
                String[] SplitArray = appHolder.getAppName().split("-");
                int executionGroup = Integer.valueOf(SplitArray[SplitArray.length - 2].substring(5));
                int parallelInstance = Integer.valueOf(SplitArray[SplitArray.length - 1]);

                logger.info("Execution Group = "+ executionGroup);
                logger.info("Parallel = "+ parallelInstance);
                logger.info("Metric details of " + appHolder.getAppName() + "\n");
                logger.info("---------------------------------------------------");

                String query = "SELECT m3 ,m5, m7 ,m16 FROM metricstable where exec=" +
                        executionGroup + " and parallel=" + parallelInstance +
                        " order by iijtimestamp desc limit 1 ";

                resultSet = statement.executeQuery(query);

                if (resultSet.isBeforeFirst()) {     //Check the corresponding partial siddhi app is having the metrics
                    logger.info("Matrics details are found for the partial Siddhi App");
                    while (resultSet.next()) {

                        double throughput = resultSet.getDouble("m3");
                        currentSummeryThroughput += throughput;
                        logger.info("Current Summary Throughput = " + currentSummeryThroughput);
                        logger.info("Throughput : " + throughput);

                        int eventCount = resultSet.getInt("m5");
                        logger.info("Event Count : " + eventCount);

                        double latency = resultSet.getLong("m7");
                        logger.info("latency : " + latency);
                        latency_map.put(appHolder.getAppName(), latency);

                        double processCPU = resultSet.getDouble("m16");
                        logger.info("process CPU : " + processCPU);
                        /*if(latency != 0.0){
                            throughput= 1/latency;
                        }else{
                            logger.info("latency=0@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@");
                        }*/

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
                logger.info("finished partial Siddhi app ......................." + appHolder.getAppName());
            }
            if ((metricCounter + 2) > appsToDeploy.size()) {
                logger.error("Metrics are not available for required number of Partial siddhi apps");
            }
        } catch (SQLException e) {
            logger.error(e);

        }
        try {
            LinkedList<PartialSiddhiApp> partialSiddhiApps = switchingFunction(currentSummeryThroughput, partailSiddhiApps);
        } catch (SQLException e) {
            e.printStackTrace();
        }
        insertPreviousData(partailSiddhiApps);


    }

    public void insertPreviousData(LinkedList<PartialSiddhiApp> partailSiddhiApps){
        double summaryThroughput = 0.0;
        int averageLatency = 0;
        int totaleventCount = 0;
        double totalProcessCPU = 0.0;
        Statement statement = null;
        Connection connection = dbConnector();

        try {
            logger.info("Autocommit enabled ? "+connection.getAutoCommit());
            logger.info("Client information" +connection.getClientInfo());
            connection.setAutoCommit(true);
            statement = connection.createStatement();
        } catch (SQLException e) {
            logger.error("Error Creating the Connection", e);
        }
        ResultSet resultSet;

        for(int i=0; i<partailSiddhiApps.size();i++){
            summaryThroughput = summaryThroughput + partailSiddhiApps.get(i).getThroughput();
        }
        logger.info("Inserting previous scheduling data in to database");
        try{
            for (int i = 0; i < partailSiddhiApps.size(); i++){
                String[] SplitArray = partailSiddhiApps.get(i).getName().split("-");
                int executionGroup = Integer.valueOf(SplitArray[SplitArray.length - 2].substring(5));
                int parallelInstance = Integer.valueOf(SplitArray[SplitArray.length - 1]);

                String sql = "INSERT INTO previous_scheduling_details (exec, parallel, SummaryThroughput, Throughput, Latency, Event_Count, process_CPU" +
                        ")" + "VALUES (" +
                        executionGroup + "," +
                        parallelInstance + "," +
                        summaryThroughput + "," +
                        partailSiddhiApps.get(i).getThroughput() + ","+
                        partailSiddhiApps.get(i).getlatency() + "," +
                        partailSiddhiApps.get(i).getEventCount() + "," +
                        partailSiddhiApps.get(i).getcpuUsage() +
                        ");";
                statement.executeUpdate(sql);
                logger.info(sql+"SQLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLL");
            }
            logger.info("Done inserting values to SQL DB");

            statement.close();
            connection.close();
        } catch(SQLException e) {
            logger.error("Error in inserting query . " + e.getMessage());
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
        logger.info("Inside Branch and Bound Knapsack ..............................................");

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
    public LinkedList<PartialSiddhiApp> switchingFunction(double currentSummeryThroughput, LinkedList<PartialSiddhiApp> partailSiddhiApps) throws SQLException {
        double previousSummaryThroughput = 0.0;
        double throughput = 0.0;
        int eventCount = 0;
        double latency = 0.0;
        double processCPU = 0.0;
        LinkedList<PartialSiddhiApp> newPartialSiddhiApps = new LinkedList<>();

        //Dictionary previousData = new Hashtable();
        Statement statement = null;
        Connection connection = dbConnector();

        try {
            logger.info("Autocommit enabled ? "+connection.getAutoCommit());
            logger.info("Client information" +connection.getClientInfo());
            connection.setAutoCommit(true);
            statement = connection.createStatement();
        } catch (SQLException e) {
            logger.error("Error Creating the Connection", e);
        }
        ResultSet resultSet;
         try{
             String query1 = "SELECT SummaryThroughput FROM previous_scheduling_details";

             resultSet = statement.executeQuery(query1);
             logger.info("Query " + query1);

             if (resultSet.isBeforeFirst()){
                 while (resultSet.next()){
                     previousSummaryThroughput = resultSet.getDouble("SummaryThroughput");
                     /*previousData.put("Throughput", resultSet.getDouble("Throughput"));
                     previousData.put("Process CPU", resultSet.getInt("process_CPU"));*/
                 }
             }
             logger.info("Previous summary throughput ################################################ = " + previousSummaryThroughput);

         }catch (SQLException e) {
             logger.error(e);
         }
         if ((previousSummaryThroughput - currentSummeryThroughput) > 5.0){
             for (int i = 0; i < partailSiddhiApps.size(); i++ ){
                 String[] SplitArray = partailSiddhiApps.get(i).getName().split("-");
                 int executionGroup = Integer.valueOf(SplitArray[SplitArray.length - 2].substring(5));

                 String query = "SELECT Throughput, Latency, Event_Count, process_CPU FROM previous_scheduling_details where exec = "+executionGroup + "";
                 resultSet = statement.executeQuery(query);
                 logger.info("Query " + query);

                 if (resultSet.isBeforeFirst()){
                     while (resultSet.next()){
                         throughput = resultSet.getDouble("Throughput");
                         eventCount = resultSet.getInt("Event_Count");
                         latency = resultSet.getLong("Latency");
                         processCPU = resultSet.getDouble("process_CPU");
                     }
                 }
                 newPartialSiddhiApps.add(new PartialSiddhiApp(processCPU,
                         latency,
                         throughput,
                         eventCount,
                         partailSiddhiApps.get(i).getName()
                 ));
             }
             //partailSiddhiApps.clear();
             partailSiddhiApps = newPartialSiddhiApps;
             newPartialSiddhiApps.clear();
         }
         statement.executeUpdate("TRUNCATE previous_scheduling_details");
         statement.close();
         connection.close();
         return partailSiddhiApps;
         }
}
