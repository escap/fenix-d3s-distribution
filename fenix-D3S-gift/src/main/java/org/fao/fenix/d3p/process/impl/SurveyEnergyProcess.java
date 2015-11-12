package org.fao.fenix.d3p.process.impl;


import org.fao.fenix.commons.msd.dto.data.Resource;
import org.fao.fenix.commons.msd.dto.full.*;
import org.fao.fenix.commons.msd.dto.type.DataType;
import org.fao.fenix.commons.msd.dto.type.RepresentationType;
import org.fao.fenix.commons.utils.Language;
import org.fao.fenix.commons.utils.database.DatabaseUtils;
import org.fao.fenix.d3p.dto.IteratorStep;
import org.fao.fenix.d3p.dto.Step;
import org.fao.fenix.d3p.dto.StepFactory;
import org.fao.fenix.d3p.dto.StepType;
import org.fao.fenix.d3p.process.impl.dto.EnergyFilterParameters;
import org.fao.fenix.d3p.process.impl.dto.PopulationFoodFilterParameters;
import org.fao.fenix.d3p.process.type.ProcessName;
import org.fao.fenix.d3s.cache.dto.dataset.Table;
import org.fao.fenix.d3s.cache.manager.impl.level1.LabelDataIterator;
import org.fao.fenix.d3s.msd.services.spi.Resources;
import org.fao.fenix.d3s.server.dto.DatabaseStandards;

import javax.inject.Inject;
import java.sql.Connection;
import java.sql.ResultSet;
import java.util.*;

@ProcessName("giftEnergyPercentage")
public class SurveyEnergyProcess extends org.fao.fenix.d3p.process.Process<EnergyFilterParameters> {
    private @Inject DatabaseUtils databaseUtils;
    private @Inject StepFactory stepFactory;
    private @Inject Resources resourcesService;


    @Override
    public Step process(Connection connection, EnergyFilterParameters params, Step... sourceStep) throws Exception {
        Step source = sourceStep!=null && sourceStep.length==1 ? sourceStep[0] : null;
        StepType type = source!=null ? source.getType() : null;
        if (type==null || type!=StepType.table)
            throw new UnsupportedOperationException("Survey summary can be applied only on a table");
        String tableName = source!=null ? (String)source.getData() : null;
        DSDDataset dsd = source!=null ? source.getDsd() : null;
        if (tableName!=null && dsd!=null) {
            //Normalize table name
            tableName = getCacheStorage().getTableName(tableName);
            double consumedEnergy = 0;
            double totalEnergy = 0;
            Collection<Object> queryParams = new LinkedList<>();
            StringBuilder query;
            String selectorSegment;
            ResultSet resultSet;

            //Consumed energy
            if (params.consumers) {
                queryParams.clear();
                query = new StringBuilder("select sum(energy) as total from ( select subject, sum(value) as energy from ").append(tableName).append(" where item = 'ENERGY'");
                selectorSegment = getPopulationSelector(params, queryParams);
                if (selectorSegment!=null)
                    query.append(" and ").append(selectorSegment);
                selectorSegment = getFoodSelector(params, queryParams);
                if (selectorSegment!=null)
                    query.append(" and ").append(selectorSegment);
                query.append(" group by subject having subject in ( select subject from ( select subject, sum(value) as total from ").append(tableName).append(" where item = 'FOOD_AMOUNT_PROC'");
                selectorSegment = getPopulationSelector(params, queryParams);
                if (selectorSegment!=null)
                    query.append(" and ").append(selectorSegment);
                selectorSegment = getFoodSelector(params, queryParams);
                if (selectorSegment!=null)
                    query.append(" and ").append(selectorSegment);
                query.append(" group by subject ) as subject_consumption where total>=15 ) ) consumers_energy");

                resultSet = databaseUtils.fillStatement(connection.prepareStatement(query.toString()),null,queryParams.toArray()).executeQuery();
                consumedEnergy = resultSet.next() ? resultSet.getDouble(1) : 0;
            } else {
                queryParams.clear();
                query = new StringBuilder("select sum(value) as energy from ").append(tableName).append(" where item = 'ENERGY'");
                selectorSegment = getPopulationSelector(params, queryParams);
                if (selectorSegment!=null)
                    query.append(" and ").append(selectorSegment);
                selectorSegment = getFoodSelector(params, queryParams);
                if (selectorSegment!=null)
                    query.append(" and ").append(selectorSegment);

                resultSet = databaseUtils.fillStatement(connection.prepareStatement(query.toString()),null,queryParams.toArray()).executeQuery();
                consumedEnergy = resultSet.next() ? resultSet.getDouble(1) : 0;
            }


            //Total energy
            if (params.consumers) {
                queryParams.clear();
                query = new StringBuilder("select sum(energy) as total from ( select subject, sum(value) as energy from ").append(tableName).append(" where item = 'ENERGY'");
                selectorSegment = getPopulationSelector(params, queryParams);
                if (selectorSegment!=null)
                    query.append(" and ").append(selectorSegment);
                query.append(" group by subject having subject in ( select subject from ( select subject, sum(value) as total from ").append(tableName).append(" where item = 'FOOD_AMOUNT_PROC'");
                selectorSegment = getPopulationSelector(params, queryParams);
                if (selectorSegment!=null)
                    query.append(" and ").append(selectorSegment);
                selectorSegment = getFoodSelector(params, queryParams);
                if (selectorSegment!=null)
                    query.append(" and ").append(selectorSegment);
                query.append(" group by subject ) as subject_consumption where total>=15 ) ) consumers_energy");

                resultSet = databaseUtils.fillStatement(connection.prepareStatement(query.toString()),null,queryParams.toArray()).executeQuery();
                totalEnergy = resultSet.next() ? resultSet.getDouble(1) : 0;
            } else {
                queryParams.clear();
                query = new StringBuilder("select sum(value) as energy from ").append(tableName).append(" where item = 'ENERGY'");
                selectorSegment = getPopulationSelector(params, queryParams);
                if (selectorSegment!=null)
                    query.append(" and ").append(selectorSegment);

                resultSet = databaseUtils.fillStatement(connection.prepareStatement(query.toString()),null,queryParams.toArray()).executeQuery();
                totalEnergy = resultSet.next() ? resultSet.getDouble(1) : 0;
            }


            //Build response data
            MeIdentification<DSDDataset> metadata = getMetadata(DatabaseStandards.getLanguageInfo());
            Iterator<Object[]> data = new LinkedList<>(Arrays.asList(
                new Object[][]{
                        {"Selected food group", consumedEnergy, "kcal"},
                        {"Other", totalEnergy-consumedEnergy, "kcal"}
                }
            )).iterator();


            //Create and return step
            IteratorStep step = (IteratorStep)stepFactory.getInstance(StepType.iterator);
            step.setRid(getRandomTmpTableName());
            step.setDsd(metadata.getDsd());
            step.setData(new LabelDataIterator(data,new Table(metadata),metadata.getDsd(),getCodeLists(new String[][] {{"GIFT_UM",null}})));
            return step;
        } else
            throw new Exception ("Source step for data filtering is unavailable or incomplete.");
    }

    private Collection<Resource<DSDCodelist, Code>> getCodeLists(String[][] ids) throws Exception {
        Collection<Resource<DSDCodelist, Code>> codeLists = new LinkedList<>();
        for (String[] id : ids)
            codeLists.add(resourcesService.loadResource(id[0],id[1]));
        return codeLists;
    }

    //DSD creator
    private MeIdentification<DSDDataset> getMetadata(Language[] languages) {
        OjCodeList umCodeList = new OjCodeList();
        umCodeList.setIdCodeList("GIFT_UM");

        MeIdentification<DSDDataset> metadata = new MeIdentification<>();
        metadata.setUid(getRandomTmpTableName());
        metadata.setMeContent(new MeContent());
        metadata.getMeContent().setResourceRepresentationType(RepresentationType.dataset);

        DSDDataset dsd = new DSDDataset();
        dsd.setContextSystem("D3P");
        dsd.setColumns(new LinkedList<DSDColumn>());
        DSDColumn column = new DSDColumn();
        column.setId("variable");
        column.setTitle(toLabel(new String[][]{{"EN", "Food group"}}));
        column.setSubject("item");
        column.setDataType(DataType.text);
        column.setKey(true);
        dsd.getColumns().add(column);
        column = new DSDColumn();
        column.setId("value");
        column.setTitle(toLabel(new String[][]{{"EN", "Energy"}}));
        column.setDataType(DataType.number);
        column.setKey(false);
        dsd.getColumns().add(column);
        column = new DSDColumn();
        column.setId("um");
        column.setTitle(toLabel(new String[][]{{"EN", "Unit of measure"}}));
        column.setDataType(DataType.code);
        column.setKey(false);
        column.setDomain(new DSDDomain());
        column.getDomain().setCodes(new LinkedList<OjCodeList>());
        column.getDomain().getCodes().add(umCodeList);
        dsd.getColumns().add(column);
        metadata.setDsd(languages!=null && languages.length>0 ? dsd.extend(languages) : dsd);

        return metadata;
    }
    private Map<String,String> toLabel (String[][] label) {
        Map<String,String> labelMap = new HashMap<>();
        if (label!=null)
            for (String[] text : label)
                labelMap.put(text[0],text[1]);
        return labelMap.size()>0 ? labelMap : null;
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



}

