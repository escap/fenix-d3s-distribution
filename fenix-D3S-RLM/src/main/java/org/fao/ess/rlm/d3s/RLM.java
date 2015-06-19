package org.fao.ess.rlm.d3s;

import org.fao.fenix.commons.msd.dto.full.DSDColumn;
import org.fao.fenix.commons.msd.dto.full.DSDDataset;
import org.fao.fenix.commons.msd.dto.full.MeIdentification;
import org.fao.fenix.commons.utils.database.DataIterator;
import org.fao.fenix.commons.utils.database.DatabaseUtils;
import org.fao.fenix.d3s.wds.dataset.WDSDatasetDao;

import javax.inject.Inject;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Iterator;
import java.util.Map;

public class RLM extends WDSDatasetDao {
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
        Connection connection = dataSource.getConnection();
        try {
            PreparedStatement statement = connection.prepareStatement(buildQuery(resource));
            statement.setString(1,resource.getUid());

            return new DataIterator(statement.executeQuery(),connection,null,null);
        } catch (Exception ex) {
            try { connection.close(); } catch (SQLException e) { }
            throw ex;
        }
    }

    @Override
    public void storeData(MeIdentification resource, Iterator<Object[]> data, boolean overwrite) throws Exception {
        throw new UnsupportedOperationException();
    }

    @Override
    public void deleteData(MeIdentification resource) throws Exception {
        throw new UnsupportedOperationException();
    }

    //Utils
    private String buildQuery(MeIdentification<DSDDataset> resource) {
        StringBuilder select = new StringBuilder();

        for (DSDColumn column : resource.getDsd().getColumns())
            select.append(",\"").append(column.getId()).append('"');

        return "select '"+resource.getUid()+"' "+select.substring(1)+" FROM master WHERE indicator = ?";
    }

}
