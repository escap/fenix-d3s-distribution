package org.fao.fenix.d3p.process.impl.dto;

import java.util.Collection;

public class PopulationFoodFilterParameters {
    public String gender,specialCondition;
    public Integer ageFrom, ageTo;
    public boolean ageYear = true;
    public Collection<String> food;

}
