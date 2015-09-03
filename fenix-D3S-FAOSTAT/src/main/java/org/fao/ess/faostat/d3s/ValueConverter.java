package org.fao.ess.faostat.d3s;

public class ValueConverter {

    private ValueType type;

    public ValueConverter(ValueType type) {
        this.type = type;
    }

    public Object apply(String source) {
        if (source!=null)
            switch (type) {
                case INT: return new Integer(source);
                case DOUBLE: return new Double(source);
                default: return source;
            }
        return null;
    }
}
