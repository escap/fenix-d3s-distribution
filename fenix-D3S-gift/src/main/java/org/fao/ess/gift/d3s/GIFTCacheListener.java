package org.fao.ess.gift.d3s;

import org.fao.fenix.d3s.cache.manager.listener.Context;
import org.fao.fenix.d3s.cache.manager.listener.DatasetAccessInfo;
import org.fao.fenix.d3s.cache.manager.listener.DatasetCacheListener;

import java.sql.SQLException;

@Context({"gift_processing"})
public class GIFTCacheListener implements DatasetCacheListener {

    @Override
    public boolean updating(DatasetAccessInfo datasetInfo) throws Exception {
        datasetInfo.getConnection().createStatement().executeUpdate("create index on "+datasetInfo.getTableName()+" (subject)");
        datasetInfo.getConnection().createStatement().executeUpdate("create index on "+datasetInfo.getTableName()+" (gender, special_condition, age_year)");
        datasetInfo.getConnection().createStatement().executeUpdate("create index on "+datasetInfo.getTableName()+" (gender, special_condition, age_month)");
        datasetInfo.getConnection().createStatement().executeUpdate("create index on "+datasetInfo.getTableName()+" (item, gender, special_condition, age_year)");
        datasetInfo.getConnection().createStatement().executeUpdate("create index on "+datasetInfo.getTableName()+" (item, gender, special_condition, age_month)");
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
