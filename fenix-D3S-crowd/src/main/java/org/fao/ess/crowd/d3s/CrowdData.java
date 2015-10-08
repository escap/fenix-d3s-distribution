package org.fao.ess.crowd.d3s;

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

public class CrowdData extends WDSDatasetDao {
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

        Integer gaulCode = getGaulCode(resource.getUid());
        if (gaulCode==null)
            return new LinkedList<Object[]>().iterator();

        Connection connection = dataSource.getConnection();
        try {
            PreparedStatement statement = connection.prepareStatement(buildQuery(columns));
            statement.setInt(1, gaulCode);

            return new DataIterator(statement.executeQuery(),connection,null,null);
        } finally {
            try { connection.close(); } catch (SQLException e) { }
        }
    }

    @Override
    public void storeData(MeIdentification resource, Iterator<Object[]> data, boolean overwrite) throws Exception {
        throw new UnsupportedOperationException("Operation not supported");
    }


    @Override
    public void deleteData(MeIdentification resource) throws Exception {
        throw new UnsupportedOperationException("Operation not supported");
    }



    //Utils
    private Integer getGaulCode(String uid) {
        uid = uid!=null ? uid.replace('.','_').toLowerCase() : null;
        try {
            return uid != null && uid.startsWith("crowd_") ? new Integer(uid.substring("crowd_".length())) : null;
        } catch (NumberFormatException ex) {
            return null;
        }
    }

    private String buildQuery(Collection<DSDColumn> columns) {
        return "select city.name as city, market.name as market, source.vendorname, source.lat, source.lon, to_char(source.fulldate,'YYYYMMDDHH24MISS') as fulldate, commodity.name||'('||source.commoditycode||')' as commodity, source.quantity, munit.name as quantity_um, source.price, currency.name as currency from (select citycode,marketcode,munitcode,currencycode,commoditycode,vendorname,lat,lon,fulldate,quantity,price from data where gaul0code = ?) as source join city on (citycode = city.code) join market on (marketcode = (''||market.code)) join munit on (munitcode = munit.code) join currency on (currencycode = currency.code) join commodity on (commoditycode = commodity.code)";
    }

}
