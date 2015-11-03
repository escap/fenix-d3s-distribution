package org.fao.ess.gift.d3s;

import org.fao.fenix.d3s.cache.manager.listener.Context;
import org.fao.fenix.d3s.cache.manager.listener.DatasetAccessInfo;
import org.fao.fenix.d3s.cache.manager.listener.DatasetCacheListener;

@Context({"gift"})
public class GIFTCacheListener implements DatasetCacheListener {

    @Override
    public boolean created(DatasetAccessInfo datasetInfo) {
        System.out.println("GIFT 1: created");
        System.out.println(datasetInfo);
        return false;
    }

    @Override
    public boolean updating(DatasetAccessInfo datasetInfo) {
        System.out.println("GIFT 1: updating");
        System.out.println(datasetInfo);
        return false;
    }

    @Override
    public boolean updated(DatasetAccessInfo datasetInfo) {
        System.out.println("GIFT 1: updated");
        System.out.println(datasetInfo);
        return false;
    }

    @Override
    public boolean removing(DatasetAccessInfo datasetInfo) {
        System.out.println("GIFT 1: removing");
        System.out.println(datasetInfo);
        return false;
    }
}
