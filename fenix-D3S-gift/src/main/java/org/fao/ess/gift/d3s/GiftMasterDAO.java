package org.fao.ess.gift.d3s;

import org.fao.fenix.commons.msd.dto.full.DSD;
import org.fao.fenix.commons.msd.dto.full.DSDColumn;
import org.fao.fenix.commons.msd.dto.full.DSDDataset;
import org.fao.fenix.commons.msd.dto.full.MeIdentification;
import org.fao.fenix.commons.utils.database.DataIterator;
import org.fao.fenix.commons.utils.database.DatabaseUtils;
import org.fao.fenix.d3s.wds.dataset.WDSDatasetDao;

import javax.inject.Inject;
import javax.transaction.HeuristicRollbackException;
import java.sql.*;
import java.util.*;

public class GiftMasterDAO extends WDSDatasetDao {
    //private static final String[] masterTableColumns = new String[]{ "survey_code", "survey", "adm0_code", "adm1_code", "adm2_code", "gps", "weighting_factor", "household", "subject", "gender", "birth_date", "age_year", "age_month", "first_ant_date", "weight", "height", "method_first_weight", "method_first_height", "second_ant_date", "sweight", "sheight", "method_second_weight", "method_second_height", "special_diet", "special_condition", "energy_intake", "unoverrep", "activity", "education", "ethnic", "profession", "comments", "survey_day", "consumption_date", "week_day", "exception_day", "consumption_time", "meal", "place", "eat_seq", "food_type", "recipe_code", "recipe_descr", "amount_recipe", "code_ingredient", "ingredient", "foodex_description", "foodex2_code", "facet_a", "facet_b", "facet_c", "facet_d", "facet_e", "facet_f", "facet_g", "item", "value", "um" };
    private static final String[] masterTableColumns = new String[]{ "adm0_code", "adm2_code", "gps", "weighting_factor", "household", "subject", "gender", "birth_date", "age_year", "age_month", "first_ant_date", "weight", "height", "method_first_weight", "method_first_height", "second_ant_date", "sweight", "sheight", "method_second_weight", "method_second_height", "special_diet", "special_condition", "energy_intake", "unoverrep", "activity", "education", "ethnic", "profession", "survey_day", "consumption_date", "week_day", "exception_day", "consumption_time", "meal", "place", "eat_seq", "food_type", "recipe_code", "recipe_descr", "amount_recipe", "ingredient", "foodex2_code", "facet_a", "facet_b", "facet_c", "facet_d", "facet_e", "facet_f", "facet_g", "item", "value", "um" };
    //private static final int[] masterTableColumnsJdbcType = new int[]{ Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.REAL, Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.DATE, Types.REAL, Types.REAL, Types.DATE, Types.REAL, Types.REAL, Types.VARCHAR, Types.VARCHAR, Types.DATE, Types.REAL, Types.REAL, Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.REAL, Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.INTEGER, Types.DATE, Types.VARCHAR, Types.VARCHAR, Types.TIMESTAMP, Types.VARCHAR, Types.VARCHAR, Types.INTEGER, Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.REAL, Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.REAL, Types.VARCHAR };
    private static final int[] masterTableColumnsJdbcType = new int[]{Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.REAL, Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.DATE, Types.REAL, Types.REAL, Types.DATE, Types.REAL, Types.REAL, Types.VARCHAR, Types.VARCHAR, Types.DATE, Types.REAL, Types.REAL, Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.REAL, Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.INTEGER, Types.DATE, Types.VARCHAR, Types.VARCHAR, Types.TIMESTAMP, Types.VARCHAR, Types.VARCHAR, Types.INTEGER, Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.REAL, Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.REAL, Types.VARCHAR};

    private static final Map<String, Integer> masterTableColumnsIndex = new HashMap<>();
    private static String insertQueryString;
    static {
        StringBuilder namesBuffer = new StringBuilder();
        StringBuilder parametersBuffer = new StringBuilder();
        for (int i=0; i<masterTableColumns.length; i++) {
            masterTableColumnsIndex.put(masterTableColumns[i], i);
            namesBuffer.append(',').append(masterTableColumns[i]);
            parametersBuffer.append(",?");
        }
        insertQueryString = "INSERT INTO master (" + namesBuffer.substring(1) + ") VALUES (" + parametersBuffer.substring(1) + ")";
    }


    @Inject private DataSource dataSource;
    @Inject private DatabaseUtils databaseUtils;
    private boolean initialized = false;



    @Override
    public boolean init() {
        return !initialized;
    }

    @Override
    public void init(Map<String, String> properties) throws Exception {
        if (!initialized)
            dataSource.init(properties.get("url"),properties.get("usr"),properties.get("psw"));
        initialized = true;
    }

    @Override
    public Iterator<Object[]> loadData(MeIdentification resource) throws Exception {
        DSD dsd = resource!=null ? resource.getDsd() : null;
        Collection<DSDColumn> columns = dsd!=null && dsd instanceof DSDDataset ? ((DSDDataset)dsd).getColumns() : null;
        if (columns==null)
            throw new Exception("Wrong table structure");

        String survey = getSurvey(resource.getUid());
        if (survey==null)
            return new LinkedList<Object[]>().iterator();

        Connection connection = dataSource.getConnection();
        connection.setAutoCommit(false);
        PreparedStatement statement = connection.prepareStatement(buildQuery(columns), ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY, ResultSet.FETCH_FORWARD);
        statement.setString(1, survey);
        statement.setFetchSize(100);

        return new DataIterator(statement.executeQuery(),connection,null,null);
    }

    @Override
    public void storeData(MeIdentification resource, Iterator<Object[]> data, boolean overwrite) throws Exception {
        if (!overwrite)
            throw new UnsupportedOperationException();

        String survey = getSurvey(resource.getUid());
        if (survey==null)
            throw new UnsupportedOperationException("Operation supported only for survey datasets");


        Connection connection = dataSource.getConnection();
        try {
            connection.setAutoCommit(false);

            //Remove existing data
            PreparedStatement statement = connection.prepareStatement("DELETE FROM master WHERE survey_code = ?");
            statement.setString(1, survey);
            statement.executeUpdate();
            statement.close();

            //Insert new data
            statement = connection.prepareStatement(insertQueryString);
            while (data.hasNext()) {
                Object[] row = buildRow(resource, data.next());
                for (int i=0; i<row.length; i++)
                    statement.setObject(i+1, row[i], masterTableColumnsJdbcType[i]);
                statement.addBatch();
            }
            statement.executeBatch();
            statement.close();

            //Commit changes
            connection.commit();
        } catch (Exception ex) {
            connection.rollback();
            throw ex;
        } finally {
            try {
                connection.setAutoCommit(true);
                connection.close();
            } catch (SQLException e) { }
        }
    }


    @Override
    public void deleteData(MeIdentification resource) throws Exception {
        String survey = getSurvey(resource.getUid());
        if (survey==null)
            throw new UnsupportedOperationException("Operation supported only for survey datasets");

        Connection connection = dataSource.getConnection();
        try {
            PreparedStatement statement = connection.prepareStatement("DELETE FROM master WHERE survey_code = ?");
            statement.setString(1, survey);
            statement.executeUpdate();
        } finally {
            try { connection.close(); } catch (SQLException e) { }
        }
    }



    //Utils
    private String getSurvey(String uid) {
        uid = uid!=null ? uid.replace('.','_') : null;
        return uid!=null && uid.toLowerCase().startsWith("gift_") ? uid.substring("gift_".length()) : null;
    }

    private String buildQuery(Collection<DSDColumn> columns) {
        StringBuilder select = new StringBuilder();

        for (DSDColumn column : columns) {
            String item = column.getId();
            int type = masterTableColumnsJdbcType[masterTableColumnsIndex.get(item)];
            if (type==Types.DATE)
                item = "to_number(to_char("+item+", 'YYYYMMDD'), '99999999')";
            else if (type==Types.TIMESTAMP)
                item = "to_number(to_char("+item+", 'YYYYMMDDHH24MISS'), '99999999999999')";
            select.append(',').append(item);
        }


        return "select "+select.substring(1)+" FROM master WHERE survey_code = ?";
    }

    private Object[] buildRow (MeIdentification<DSDDataset> resource, Object[] rawRow) {
        Object[] row = new Object[masterTableColumns.length];
        //Add dataset row data
        int i=0;
        for (DSDColumn columnMetadata : resource.getDsd().getColumns())
            row[masterTableColumnsIndex.get(columnMetadata.getId().toLowerCase())] = rawRow[i++];
        //Return created row
        return row;
    }



}
