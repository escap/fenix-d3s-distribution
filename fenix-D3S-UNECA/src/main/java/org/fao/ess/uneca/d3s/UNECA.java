package org.fao.ess.uneca.d3s;

import org.fao.fenix.commons.msd.dto.templates.standard.dsd.DSDColumn;
import org.fao.fenix.commons.msd.dto.templates.standard.combined.dataset.MetadataDSD;
import org.fao.fenix.commons.utils.database.DataIterator;
import org.fao.fenix.commons.utils.database.DatabaseUtils;
import org.fao.fenix.d3s.wds.dataset.DatasetStructure;
import org.fao.fenix.d3s.wds.dataset.WDSDatasetDao;

import javax.inject.Inject;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Iterator;
import java.util.Map;

public class UNECA extends WDSDatasetDao {
    @Inject DataSource dataSource;
    @Inject DatabaseUtils databaseUtils;
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

    //
    @Override
    protected Iterator<Object[]> loadData(MetadataDSD resource, DatasetStructure structure) throws Exception {
        Connection connection = null;
        try {
            connection = dataSource.getConnection();
            PreparedStatement statement = connection.prepareStatement(buildQuery(resource));
            statement.setString(1,resource.getUid());

            return new DataIterator(statement.executeQuery(),connection,null,null);
        } catch (Exception ex) {
            if (connection!=null)
                try { connection.close(); } catch (SQLException e) { }
            throw ex;
        }
    }

    @Override
    protected void storeData(MetadataDSD resource, Iterator<Object[]> data, boolean overwrite, DatasetStructure structure) throws Exception {
        throw new UnsupportedOperationException();
    }

    @Override
    public void deleteData(MetadataDSD resource) throws Exception {
        throw new UnsupportedOperationException();
    }

    //Utils
    private String buildQuery(MetadataDSD resource) {
        StringBuilder select = new StringBuilder();

        for (DSDColumn column : resource.getDsd().getColumns())
            select.append(",\"").append(column.getId()).append('"');

        return "select "+select.substring(1)+" FROM data WHERE \"Metadata_ID\" = ?";
    }

}
