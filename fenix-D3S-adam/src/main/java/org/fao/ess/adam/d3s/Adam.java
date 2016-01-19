package org.fao.ess.adam.d3s;

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

public class Adam extends WDSDatasetDao {

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

        String topic = getAmountType(resource.getUid());
        if (topic==null)
            return new LinkedList<Object[]>().iterator();

        Connection connection = dataSource.getConnection();
        return new DataIterator(connection.createStatement().executeQuery(buildQuery(columns, topic)),connection,null,buildNulls(columns));
    }

    @Override
    public void storeData(MeIdentification resource, Iterator<Object[]> data, boolean overwrite) throws Exception {
        throw new UnsupportedOperationException();
    }


    @Override
    public void deleteData(MeIdentification resource) throws Exception {
        throw new UnsupportedOperationException("Operation supported only for topic datasets");
    }



    //Utils
    private String getAmountType(String uid) {
        uid = uid!=null ? uid.replace('.','_').toLowerCase() : null;
        return uid!=null && uid.startsWith("adam_") ? uid.substring("adam_".length()) : null;
    }

    private Object[] buildNulls(Collection<DSDColumn> columns) {
        int i=0;
        Object[] nullValues = new Object[columns.size()];
        for (DSDColumn column : columns)
            nullValues[i++] = column.getKey()!=null && column.getKey() ? "NA" : null;
        return nullValues;
    }

    private String buildQuery(Collection<DSDColumn> columns, String amounttype   ) {
        StringBuilder select = new StringBuilder();

        for (DSDColumn column : columns)
            select.append(',').append(column.getId());

        return "select "+select.substring(1)+" FROM "+amounttype;
    }



}
