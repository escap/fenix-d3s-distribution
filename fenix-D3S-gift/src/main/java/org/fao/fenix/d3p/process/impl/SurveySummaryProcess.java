package org.fao.fenix.d3p.process.impl;


import org.fao.fenix.commons.msd.dto.full.DSDColumn;
import org.fao.fenix.commons.msd.dto.full.DSDDataset;
import org.fao.fenix.commons.msd.dto.type.DataType;
import org.fao.fenix.commons.utils.database.DatabaseUtils;
import org.fao.fenix.d3p.dto.*;
import org.fao.fenix.d3p.process.impl.dto.Percentile;
import org.fao.fenix.d3p.process.impl.dto.PopulationFoodFilterParameters;
import org.fao.fenix.d3p.process.impl.dto.SummaryFilterParameters;
import org.fao.fenix.d3p.process.type.ProcessName;

import javax.inject.Inject;
import java.sql.Connection;
import java.sql.ResultSet;
import java.util.*;

@ProcessName("giftSurveySummary")
public class SurveySummaryProcess extends org.fao.fenix.d3p.process.Process<SummaryFilterParameters> {
    private @Inject DatabaseUtils databaseUtils;
    private @Inject StepFactory stepFactory;


    @Override
    public Step process(Connection connection, SummaryFilterParameters params, Step... sourceStep) throws Exception {
        Step source = sourceStep!=null && sourceStep.length==1 ? sourceStep[0] : null;
        StepType type = source!=null ? source.getType() : null;
        if (type==null || type!=StepType.table)
            throw new UnsupportedOperationException("Survey summary can be applied only on a table");
        String tableName = source!=null ? (String)source.getData() : null;
        DSDDataset dsd = source!=null ? source.getDsd() : null;
        if (tableName!=null && dsd!=null) {
            //Normalize table name
            tableName = getCacheStorage().getTableName(tableName);

            //Population and consumers number
            Collection<Object> queryParams = new LinkedList<>();
            StringBuilder query = new StringBuilder("SELECT COUNT(*) FROM (SELECT DISTINCT subject FROM ").append(tableName).append(" where item = 'FOOD_AMOUNT_PROC'");
            String selectorSegment = getPopulationSelector(params, queryParams);
            if (selectorSegment!=null)
                query.append(" and ").append(selectorSegment);
            query.append(" ) AS temp");

            ResultSet resultSet = databaseUtils.fillStatement(connection.prepareStatement(query.toString()),null,queryParams.toArray()).executeQuery();
            int subjectsNumber = resultSet.next() ? resultSet.getInt(1) : 0;

            queryParams.clear();
            query = new StringBuilder("select count(*) as count from ( select subject, sum(value) as total from ").append(tableName).append(" where item = 'FOOD_AMOUNT_PROC'");
            selectorSegment = getPopulationSelector(params, queryParams);
            if (selectorSegment!=null)
                query.append(" and ").append(selectorSegment);
            selectorSegment = getFoodSelector(params, queryParams);
            if (selectorSegment!=null)
                query.append(" and ").append(selectorSegment);
            query.append(" group by subject ) as subject_consumption where total>=15");

            resultSet = databaseUtils.fillStatement(connection.prepareStatement(query.toString()),null,queryParams.toArray()).executeQuery();
            int consumersNumber = resultSet.next() ? resultSet.getInt(1) : 0;

            //Population consumed quantity percentage
            queryParams.clear();
            query = new StringBuilder("select sum (value) from ").append(tableName).append(" where item = 'FOOD_AMOUNT_PROC'");
            selectorSegment = getPopulationSelector(params, queryParams);
            if (selectorSegment!=null)
                query.append(" and ").append(selectorSegment);

            resultSet = databaseUtils.fillStatement(connection.prepareStatement(query.toString()),null,queryParams.toArray()).executeQuery();
            double totalConsumedQuantity = resultSet.next() ? resultSet.getDouble(1) : 0;

            selectorSegment = getFoodSelector(params, queryParams);
            if (selectorSegment!=null)
                query.append(" and ").append(selectorSegment);

            resultSet = databaseUtils.fillStatement(connection.prepareStatement(query.toString()),null,queryParams.toArray()).executeQuery();
            double foodConsumedQuantity = resultSet.next() ? resultSet.getDouble(1) : 0;

            double foodConsumedPercentage = totalConsumedQuantity>0 ? (foodConsumedQuantity/totalConsumedQuantity)*100 : 0;

            //Mean consumption, minimum consumpition and maximum consumption
            queryParams.clear();
            query = new StringBuilder("select min(total), max(total), avg(total) from ( select subject, sum(value) as total from ").append(tableName).append(" where item = 'FOOD_AMOUNT_PROC'");
            selectorSegment = getPopulationSelector(params, queryParams);
            if (selectorSegment!=null)
                query.append(" and ").append(selectorSegment);
            selectorSegment = getFoodSelector(params, queryParams);
            if (selectorSegment!=null)
                query.append(" and ").append(selectorSegment);
            query.append(" group by subject ) as bySubject");

            resultSet = databaseUtils.fillStatement(connection.prepareStatement(query.toString()),null,queryParams.toArray()).executeQuery();
            double populationMeanConsumption=0,populationMinConsumption=0,populationMaxConsumption=0;
            if (resultSet.next()) {
                populationMinConsumption = resultSet.getDouble(1);
                populationMaxConsumption = resultSet.getDouble(2);
                populationMeanConsumption = resultSet.getDouble(3);
            }

            query.append(" where total>=15"); //consumers only

            resultSet = databaseUtils.fillStatement(connection.prepareStatement(query.toString()),null,queryParams.toArray()).executeQuery();
            double consumersMeanConsumption=0,consumersMinConsumption=0,consumersMaxConsumption=0;
            if (resultSet.next()) {
                consumersMinConsumption = resultSet.getDouble(1);
                consumersMaxConsumption = resultSet.getDouble(2);
                consumersMeanConsumption = resultSet.getDouble(3);
            }

            //Population standard deviation
            queryParams.clear();
            query = new StringBuilder("select sum((total-").append(populationMeanConsumption).append(")*(total-").append(populationMeanConsumption).append(")) as value from ( select subject, sum(value) as total from ").append(tableName).append(" where item = 'FOOD_AMOUNT_PROC'");
            selectorSegment = getPopulationSelector(params, queryParams);
            if (selectorSegment!=null)
                query.append(" and ").append(selectorSegment);
            selectorSegment = getFoodSelector(params, queryParams);
            if (selectorSegment!=null)
                query.append(" and ").append(selectorSegment);
            query.append(" group by subject ) as subject_consumption");

            resultSet = databaseUtils.fillStatement(connection.prepareStatement(query.toString()),null,queryParams.toArray()).executeQuery();
            Double populationStandardDeviation = resultSet.next() ? resultSet.getDouble(1) : 0;
            populationStandardDeviation = subjectsNumber>1 ? Math.sqrt(populationStandardDeviation/(subjectsNumber-1)) : null;

            //Consumers standard deviation
            queryParams.clear();
            query = new StringBuilder("select sum((total-").append(consumersMeanConsumption).append(")*(total-").append(consumersMeanConsumption).append(")) as value from ( select subject, sum(value) as total from ").append(tableName).append(" where item = 'FOOD_AMOUNT_PROC'");
            selectorSegment = getPopulationSelector(params, queryParams);
            if (selectorSegment!=null)
                query.append(" and ").append(selectorSegment);
            selectorSegment = getFoodSelector(params, queryParams);
            if (selectorSegment!=null)
                query.append(" and ").append(selectorSegment);
            query.append(" group by subject ) as subject_consumption where total>=15");

            resultSet = databaseUtils.fillStatement(connection.prepareStatement(query.toString()),null,queryParams.toArray()).executeQuery();
            Double consumersStandardDeviation = resultSet.next() ? resultSet.getDouble(1) : 0;
            consumersStandardDeviation = subjectsNumber>1 ? Math.sqrt(consumersStandardDeviation/(consumersNumber-1)) : null;

            //Population percentiles
            int[] requiredPercentiles = params.percentiles!=null && params.percentiles.length>0 ? params.percentiles : new int[]{10, 25, 50, 75, 90, 95};
            Collection<Percentile> populationPercentiles = new LinkedList<>();

            queryParams.clear();
            query = new StringBuilder("select id, total from ( select rownum() as id, subject, total from ( select subject, sum( casewhen (").append(getFoodContainedFunction(params, queryParams)).append(", value , 0)) as total from ").append(tableName).append(" where item = 'FOOD_AMOUNT_PROC'");
            selectorSegment = getPopulationSelector(params, queryParams);
            if (selectorSegment!=null)
                query.append(" and ").append(selectorSegment);
            query.append(" group by subject order by total ) as subject_consumption ) as subject_consumption_index where ").append(getPercentileSelector(queryParams, subjectsNumber, requiredPercentiles));

            resultSet = databaseUtils.fillStatement(connection.prepareStatement(query.toString()),null,queryParams.toArray()).executeQuery();
            for (int i=0; resultSet.next() && i<requiredPercentiles.length; i++)
                populationPercentiles.add(new Percentile(requiredPercentiles[i], resultSet.getDouble(2)));

            //Consumers percentiles
            Collection<Percentile> consumersPercentiles = new LinkedList<>();

            queryParams.clear();
            query = new StringBuilder("select id, total from ( select rownum() as id, subject, total from ( select subject, sum(value) as total from ").append(tableName).append(" where item = 'FOOD_AMOUNT_PROC'");
            selectorSegment = getPopulationSelector(params, queryParams);
            if (selectorSegment!=null)
                query.append(" and ").append(selectorSegment);
            selectorSegment = getFoodSelector(params, queryParams);
            if (selectorSegment!=null)
                query.append(" and ").append(selectorSegment);
            query.append(" group by subject order by total ) as subject_consumption where total>=15 ) as subject_consumption_index where ").append(getPercentileSelector(queryParams, consumersNumber, requiredPercentiles));

            resultSet = databaseUtils.fillStatement(connection.prepareStatement(query.toString()),null,queryParams.toArray()).executeQuery();
            for (int i=0; resultSet.next() && i<requiredPercentiles.length; i++)
                consumersPercentiles.add(new Percentile(requiredPercentiles[i], resultSet.getDouble(2)));


            //Build response data
            Collection<Object[]> data = new LinkedList<>(Arrays.asList(
                    new Object[][]{
                            {"subjectsNumber", (double)subjectsNumber},
                            {"consumersNumber", (double)consumersNumber},
                            {"totalConsumedQuantity", totalConsumedQuantity},
                            {"foodConsumedQuantity", foodConsumedQuantity},
                            {"foodConsumedPercentage", foodConsumedPercentage},
                            {"populationMinConsumption", populationMinConsumption},
                            {"populationMaxConsumption", populationMaxConsumption},
                            {"populationMeanConsumption", populationMeanConsumption},
                            {"populationMaxConsumption", populationMaxConsumption},
                            {"consumersMinConsumption", consumersMinConsumption},
                            {"consumersMaxConsumption", consumersMaxConsumption},
                            {"consumersMeanConsumption", consumersMeanConsumption},
                            {"populationStandardDeviation", populationStandardDeviation},
                            {"consumersStandardDeviation", consumersStandardDeviation}
                    }
            ));
            for (Percentile percentile : populationPercentiles)
                data.add(new Object[]{"percentilePopulation_"+percentile.percentile, percentile.value});
            for (Percentile percentile : consumersPercentiles)
                data.add(new Object[]{"percentileConsumer_"+percentile.percentile, percentile.value});

            //Return step
            IteratorStep step = (IteratorStep)stepFactory.getInstance(StepType.iterator);
            step.setRid(getRandomTmpTableName());
            step.setDsd(getDsd());
            step.setData(data.iterator());
            return step;
        } else
            throw new Exception ("Source step for data filtering is unavailable or incomplete.");
    }

    //DSD creator
    private DSDDataset getDsd() {
        DSDDataset resultDSD = new DSDDataset();
        resultDSD.setContextSystem("D3S");
        resultDSD.setColumns(new LinkedList<DSDColumn>());
        DSDColumn column = new DSDColumn();
        column.setId("variable");
        column.setDataType(DataType.text);
        column.setKey(true);
        resultDSD.getColumns().add(column);
        column = new DSDColumn();
        column.setId("value");
        column.setDataType(DataType.number);
        column.setKey(false);
        resultDSD.getColumns().add(column);
        return resultDSD;
    }


    //Query snippets
    private String getPopulationSelector(PopulationFoodFilterParameters params, Collection<Object> queryParams) {
        StringBuilder buffer = new StringBuilder();
        if (params!=null) {
            if (params.gender!=null) {
                buffer.append(" and gender = ?");
                queryParams.add(params.gender);
            }
            if (params.specialCondition!=null) {
                buffer.append(" and special_condition = ?");
                queryParams.add(params.specialCondition);
            }
            if (params.ageFrom!=null || params.ageTo!=null) {
                buffer.append(" and ").append(params.ageYear?"age_year":"age_month");
                if (params.ageFrom!=null && params.ageTo!=null) {
                    buffer.append(" between ? and ?");
                    queryParams.add(params.ageFrom);
                    queryParams.add(params.ageTo);
                } else if (params.ageFrom!=null) {
                    buffer.append(" >= ?");
                    queryParams.add(params.ageFrom);
                } else {
                    buffer.append(" <= ?");
                    queryParams.add(params.ageTo);
                }
            }
        }
        return buffer.length()>0 ? buffer.substring(5) : null;
    }

    private String getFoodSelector(PopulationFoodFilterParameters params, Collection<Object> queryParams) {
        if (params!=null && params.food!=null && params.food.size()>0) {
            StringBuilder buffer = new StringBuilder("foodex2_code in (");
            for (int i=0, l=params.food.size(); i<l; i++)
                buffer.append("?,");
            buffer.setCharAt(buffer.length()-1,')');
            queryParams.addAll(params.food);
            return buffer.toString();
        }
        return null;
    }

    private String getFoodContainedFunction(PopulationFoodFilterParameters params, Collection<Object> queryParams) {
        if (params!=null && params.food!=null && params.food.size()>0) {
            StringBuilder buffer = new StringBuilder("ARRAY_CONTAINS ( (");
            for (int i=0, l=params.food.size(); i<l; i++)
                buffer.append("?,");
            buffer.setCharAt(buffer.length()-1,')');
            buffer.append(",foodex2_code )");
            queryParams.addAll(params.food);
            return buffer.toString();
        }
        return "false";
    }

    private String getPercentileSelector(Collection<Object> queryParams, int populationSize, int ... percentiles) {
        if (percentiles!=null && percentiles.length>0 && populationSize>0) {
            StringBuilder buffer = new StringBuilder("id in (");
            for (int i=0; i<percentiles.length; i++) {
                buffer.append("?,");
                queryParams.add((int)Math.ceil((((double)populationSize)/100)*percentiles[i]));
            }
            buffer.setCharAt(buffer.length()-1,')');
            return buffer.toString();
        }
        return null;
    }



}

