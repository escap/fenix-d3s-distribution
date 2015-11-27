package org.fao.ess.cache.d3s;

import org.fao.fenix.commons.msd.dto.full.MeIdentification;
import org.fao.fenix.d3s.msd.listener.MetadataListener;
import org.fao.fenix.d3s.server.init.MainController;
import org.fao.fenix.d3s.server.tools.orient.OrientDao;

import javax.inject.Inject;

public class L3ResourceCache extends OrientDao implements MetadataListener {
    @Inject MainController main;
    private boolean initialized;

    private static void init(MainController main) throws Exception {
        main.getInitParameter("cache.l3.host");
        main.getInitParameter("cache.l3.user");
        main.getInitParameter("cache.l3.password");
        main.getInitParameter("cache.l3.command");
    }

    @Override
    public boolean insert(MeIdentification metadata) throws Exception {

        return false;
    }

    @Override
    public boolean update(MeIdentification currentMetadata, MeIdentification metadata) throws Exception {
        return false;
    }

    @Override
    public boolean append(MeIdentification currentMetadata, MeIdentification metadata) throws Exception {
        return false;
    }

    @Override
    public boolean remove(MeIdentification metadata) throws Exception {
        return false;
    }
}
