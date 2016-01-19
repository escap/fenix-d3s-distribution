package org.fao.ess.cache.d3s;

import org.fao.fenix.commons.msd.dto.full.MeIdentification;
import org.fao.fenix.d3s.msd.listener.Context;
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
    public void insert (MeIdentification metadata) throws Exception {
        System.out.println("insert");
    }

    @Override
    public void update (MeIdentification currentMetadata, MeIdentification metadata) throws Exception {
        System.out.println("update");
    }

    @Override
    public void append (MeIdentification currentMetadata, MeIdentification metadata) throws Exception {
        System.out.println("append");
    }

    @Override
    public void remove (MeIdentification metadata) throws Exception {
        System.out.println("delete");
    }
}
