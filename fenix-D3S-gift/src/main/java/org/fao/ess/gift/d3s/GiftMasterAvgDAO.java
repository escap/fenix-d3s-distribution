package org.fao.ess.gift.d3s;

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

public class GiftMasterAvgDAO extends WDSDatasetDao {
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
        Statement statement = connection.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY, ResultSet.FETCH_FORWARD);
        statement.setFetchSize(100);
        String query = "select subject, adm0_code, adm1_code, gender, age_year, age_month, special_condition, foodex2_code, facet_a, facet_b, facet_c, facet_d, facet_e, facet_f, facet_g, item, value, um from master_avg where survey_code = '"+survey+'\'';
        return new DataIterator(statement.executeQuery(query),connection,null,null);
    }

    @Override
    public void storeData(MeIdentification resource, Iterator<Object[]> data, boolean overwrite) throws Exception {
        throw new UnsupportedOperationException();
    }


    @Override
    public void deleteData(MeIdentification resource) throws Exception {
        throw new UnsupportedOperationException("Operation supported only for survey datasets");
    }



    //Utils
    private String getSurvey(String uid) {
        uid = uid!=null ? uid.replace('.','_') : null;
        return uid!=null && uid.toLowerCase().startsWith("gift_avg_") ? uid.substring("gift_avg_".length()) : null;
    }



}
