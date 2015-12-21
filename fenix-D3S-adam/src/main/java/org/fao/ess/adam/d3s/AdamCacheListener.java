package org.fao.ess.adam.d3s;

import org.fao.fenix.d3s.cache.manager.listener.Context;
import org.fao.fenix.d3s.cache.manager.listener.DatasetAccessInfo;
import org.fao.fenix.d3s.cache.manager.listener.DatasetCacheListener;

@Context({"adam"})
public class AdamCacheListener implements DatasetCacheListener {

    @Override
    public boolean updating(DatasetAccessInfo datasetInfo) throws Exception {
        datasetInfo.getConnection().createStatement().executeUpdate("create index on "+datasetInfo.getTableName()+" (flowcode, sectorcode, year)");
        datasetInfo.getConnection().createStatement().executeUpdate("create index on "+datasetInfo.getTableName()+" (flowcode, sectorcode, donorcode, year)");
        datasetInfo.getConnection().createStatement().executeUpdate("create index on "+datasetInfo.getTableName()+" (flowcode, sectorcode, recipientcode, year)");
        datasetInfo.getConnection().createStatement().executeUpdate("create index on "+datasetInfo.getTableName()+" (flowcode, sectorcode, recipientcode, donorcode, year)");
        datasetInfo.getConnection().createStatement().executeUpdate("create index on "+datasetInfo.getTableName()+" (flowcode, purposecode, year)");
        datasetInfo.getConnection().createStatement().executeUpdate("create index on "+datasetInfo.getTableName()+" (flowcode, purposecode, donorcode, year)");
        datasetInfo.getConnection().createStatement().executeUpdate("create index on "+datasetInfo.getTableName()+" (flowcode, purposecode, recipientcode, year)");
        datasetInfo.getConnection().createStatement().executeUpdate("create index on "+datasetInfo.getTableName()+" (flowcode, purposecode, recipientcode, donorcode, year)");
        return false;
    }

    @Override
    public boolean updated(DatasetAccessInfo datasetInfo) throws Exception {
        return false;
    }

    @Override
    public boolean removing(DatasetAccessInfo datasetInfo) throws Exception {
        return false;
    }
}
