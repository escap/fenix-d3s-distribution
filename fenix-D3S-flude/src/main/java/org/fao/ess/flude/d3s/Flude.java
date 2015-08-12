package org.fao.ess.flude.d3s;

import org.fao.fenix.commons.msd.dto.full.DSD;
import org.fao.fenix.commons.msd.dto.full.DSDColumn;
import org.fao.fenix.commons.msd.dto.full.DSDDataset;
import org.fao.fenix.commons.msd.dto.full.MeIdentification;
import org.fao.fenix.commons.utils.database.DataIterator;
import org.fao.fenix.commons.utils.database.DatabaseUtils;
import org.fao.fenix.d3s.wds.dataset.WDSDatasetDao;

import javax.inject.Inject;
import java.sql.*;
import java.util.*;

public class Flude extends WDSDatasetDao {
    private static final String[] masterTableColumns = new String[]{ "region", "subregion", "domain", "incomes", "country", "year", "tot_pop_1000", "tot_area", "desk_study", "gdpusd2012", "indicator", "itto", "comifac", "foreur", "montreal", "unece", "value", "ts", "tt"};
    private static final int[] masterTableColumnsJdbcType = new int[]{Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.INTEGER, Types.REAL, Types.REAL, Types.VARCHAR, Types.REAL, Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.REAL, Types.VARCHAR, Types.VARCHAR};

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

        String topic = getTopic(resource.getUid());
        if (topic==null)
            return new LinkedList<Object[]>().iterator();

        Connection connection = dataSource.getConnection();
        try {
            PreparedStatement statement = connection.prepareStatement(buildQuery(columns));
            statement.setString(1, topic);

            return new DataIterator(statement.executeQuery(),connection,null,null);
        } finally {
            try { connection.close(); } catch (SQLException e) { }
        }
    }

    @Override
    public void storeData(MeIdentification resource, Iterator<Object[]> data, boolean overwrite) throws Exception {
        if (!overwrite)
            throw new UnsupportedOperationException();

        String topic = getTopic(resource.getUid());
        if (topic==null)
            throw new UnsupportedOperationException("Operation supported only for topic datasets");


        Connection connection = dataSource.getConnection();
        try {
            connection.setAutoCommit(false);

            //Remove existing data
            PreparedStatement statement = connection.prepareStatement("DELETE FROM master WHERE TOPIC = ?");
            statement.setString(1, topic);
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
        String topic = getTopic(resource.getUid());
        if (topic==null)
            throw new UnsupportedOperationException("Operation supported only for topic datasets");

        Connection connection = dataSource.getConnection();
        try {
            PreparedStatement statement = connection.prepareStatement("DELETE FROM master WHERE TOPIC = ?");
            statement.setString(1, topic);
            statement.executeUpdate();
        } finally {
            try { connection.close(); } catch (SQLException e) { }
        }
    }



    //Utils
    private String getTopic(String uid) {
        uid = uid!=null ? uid.replace('.','_').toLowerCase() : null;
        return uid!=null && uid.startsWith("rlm_topic_") ? uid.substring(10) : null;
    }

    private String buildQuery(Collection<DSDColumn> columns) {
        StringBuilder select = new StringBuilder();

        for (DSDColumn column : columns)
            select.append(',').append(column.getId());

        return "select "+select.substring(1)+" FROM master WHERE TOPIC = ?";
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
