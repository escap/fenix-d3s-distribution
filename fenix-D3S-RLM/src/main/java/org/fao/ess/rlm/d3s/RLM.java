package org.fao.ess.rlm.d3s;

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

public class RLM extends WDSDatasetDao {

    private static final String[] masterTableColumns = new String[]{"country", "year", "year_label", "indicator", "indicator_label", "qualifier", "value", "um", "source", "topic", "flag"};
    private static final int[] masterTableColumnsJdbcType = new int[]{Types.VARCHAR, Types.INTEGER, Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.VARCHAR};

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

        Connection connection = dataSource.getConnection();
        try {
            PreparedStatement statement = connection.prepareStatement(buildQuery(columns));
            statement.setString(1,getIndicator(resource.getUid()));

            return new DataIterator(statement.executeQuery(),connection,null,null);
        } finally {
            try { connection.close(); } catch (SQLException e) { }
        }
    }

    @Override
    public void storeData(MeIdentification resource, Iterator<Object[]> data, boolean overwrite) throws Exception {
        if (!overwrite)
            throw new UnsupportedOperationException();

        Connection connection = dataSource.getConnection();
        try {
            connection.setAutoCommit(false);

            //Remove existing data
            PreparedStatement statement = connection.prepareStatement("DELETE FROM master WHERE INDICATOR = ?");
            statement.setString(1, getIndicator(resource.getUid()));
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

            //Retrieve indicator label
            ResultSet resultSet = connection.createStatement().executeQuery("SELECT indicator_label FROM codes_indicators WHERE indicator_code = '"+getIndicator(resource.getUid())+"'");
            resultSet.next();
            String indicatorLabel = resultSet.getString(1);
            resultSet.close();

            //Update indicator label
            statement = connection.prepareStatement("UPDATE master SET indicator_label = ? WHERE INDICATOR = ?");
            statement.setString(1, indicatorLabel);
            statement.setString(2, getIndicator(resource.getUid()));
            statement.executeUpdate();
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
        Connection connection = dataSource.getConnection();
        try {
            PreparedStatement statement = connection.prepareStatement("DELETE FROM master WHERE INDICATOR = ?");
            statement.setString(1, getIndicator(resource.getUid()));
            statement.executeUpdate();
        } finally {
            try { connection.close(); } catch (SQLException e) { }
        }
    }



    //Utils
    private String getIndicator(String uid) {
        return uid!=null && (uid.toLowerCase().startsWith("rlm.") || uid.toLowerCase().startsWith("rlm_")) ? uid.substring(4) : uid;
    }

    private String buildQuery(Collection<DSDColumn> columns) {
        StringBuilder select = new StringBuilder();

        for (DSDColumn column : columns)
            if ("COUNTRY".equalsIgnoreCase(column.getId()))
                select.append(",''||COUNTRY AS COUNTRY");
            else
                select.append(',').append(column.getId());

        return "select "+select.substring(1)+" FROM master WHERE INDICATOR = ?";
    }

    private Object[] buildRow (MeIdentification<DSDDataset> resource, Object[] rawRow) {
        Object[] row = new Object[masterTableColumns.length];
        //Add dataset row data
        int i=0;
        for (DSDColumn columnMetadata : resource.getDsd().getColumns())
            row[masterTableColumnsIndex.get(columnMetadata.getId().toLowerCase())] = rawRow[i++];
        //Add indicator value
        row[masterTableColumnsIndex.get("indicator")] = getIndicator(resource.getUid());
        //Return created row
        return row;
    }



}
